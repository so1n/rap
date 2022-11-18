import asyncio
import logging
import random
import sys
import time
import traceback
from functools import partial
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Coroutine, Dict, Optional, Tuple, Union

from rap.common.asyncio_helper import Deadline, SetEvent, get_deadline, get_event_loop
from rap.common.conn import CloseConnException, ServerConnection
from rap.common.event import CloseConnEvent, DeclareEvent, DropEvent, PingEvent, ServerErrorEvent
from rap.common.exceptions import (
    BaseRapError,
    ChannelError,
    FuncNotFoundError,
    InvokeError,
    ParseError,
    RpcRunTimeError,
)
from rap.common.types import MSG_TYPE
from rap.common.utils import ImmutableDict, constant, parse_error
from rap.server.channel import Channel, get_corresponding_channel_class
from rap.server.model import Request, Response, ServerContext
from rap.server.plugin.processor.base import BaseProcessor, ContextExitType
from rap.server.registry import FuncModel
from rap.server.sender import Sender

if TYPE_CHECKING:
    from rap.server.core import Server

__all__ = ["Receiver"]
logger: logging.Logger = logging.getLogger(__name__)
DISPATCH_FUNC_TYPE = Callable[[Request, Response], Coroutine[Tuple[Optional[Response], bool], Any, Any]]


class Receiver(object):
    def __init__(
        self,
        app: "Server",
        conn: ServerConnection,
        run_timeout: int,
        sender: Sender,
        ping_fail_cnt: int,
        ping_sleep_time: int,
        call_func_permission_fn: Optional[Callable[[Request], Coroutine[Any, Any, FuncModel]]] = None,
        processor: Optional[BaseProcessor] = None,
    ):
        """Receive and process messages from the client, and execute different logics according to the message type
        :param app: server
        :param conn: server conn
        :param run_timeout: Maximum execution time per call
        :param sender: Send response data to the client
        :param ping_fail_cnt: When ping fails continuously and exceeds this value, conn will be disconnected
        :param ping_sleep_time: ping message interval time
        :param call_func_permission_fn: Check the permission to call the private function
        :param processor: processor
        """
        self._app: "Server" = app
        self._conn: ServerConnection = conn
        self._run_timeout: int = run_timeout
        self.sender: Sender = sender
        self.context_dict: Dict[int, ServerContext] = {}
        self._ping_sleep_time: int = ping_sleep_time
        self._ping_fail_cnt: int = ping_fail_cnt
        self._get_func_from_request: Callable[[Request], Coroutine[Any, Any, FuncModel]] = (
            call_func_permission_fn if call_func_permission_fn else self._default_call_fun_permission_fn
        )

        self.dispatch_func_dict: Dict[int, DISPATCH_FUNC_TYPE] = {
            constant.MT_CLIENT_EVENT: self.event,
            constant.MT_SERVER_EVENT: self.event,
            constant.MT_MSG: self.msg_handle,
            constant.MT_CHANNEL: self.channel_handle,
        }
        # now one conn one Request object
        self._keepalive_timestamp: int = int(time.time())
        self._metadata: dict = {}

        # processor
        self._processor: Optional[BaseProcessor] = processor

        # Store resources that cannot be controlled by the current concurrent process
        self._used_resources: SetEvent = SetEvent()
        self._server_info: ImmutableDict = ImmutableDict(
            host=self._conn.peer_tuple,
            version=constant.VERSION,
            user_agent=constant.USER_AGENT,
        )
        self._client_info: ImmutableDict = ImmutableDict()

    async def _default_call_fun_permission_fn(self, request: Request) -> FuncModel:
        func_key: str = self._app.registry.gen_key(
            request.group,
            request.func_name,
            constant.NORMAL if request.msg_type == constant.MT_MSG else constant.CHANNEL,
        )
        if func_key not in self._app.registry.func_dict:
            raise FuncNotFoundError(extra_msg=f"name: {request.func_name}")
        func_model: FuncModel = self._app.registry.func_dict[func_key]
        if func_model.is_private and request.context.conn.peer_tuple[0] not in ("::1", "127.0.0.1", "localhost"):
            raise FuncNotFoundError(f"No permission to call:`{request.func_name}`")
        return func_model

    async def get_context(self, correlation_id: int) -> ServerContext:
        context: Optional[ServerContext] = self.context_dict.get(correlation_id, None)
        if not context:
            # create context
            context = ServerContext()
            context.app = self  # type: ignore
            context.conn = self._conn
            context.correlation_id = correlation_id
            context.server_info = self._server_info
            context.client_info = self._client_info
            context.transport_metadata = self._metadata
            self.context_dict[correlation_id] = context
            if self._processor:
                await self._processor.on_context_enter(context)
        return context

    async def close_context(self, context: ServerContext):
        if context.get_value("_is_pop", False):
            raise RpcRunTimeError(f"{context} has been closed")
        self.context_dict.pop(context.correlation_id, None)
        context._is_pop = True
        if self._processor:
            exc_type, exc_val, exc_tb = sys.exc_info()

            async def _context_exit_cb() -> ContextExitType:
                return context, exc_type, exc_val, exc_tb

            await self._processor.on_context_exit(_context_exit_cb)

    async def __call__(self, request_msg: Optional[MSG_TYPE]) -> None:
        if request_msg is None:
            await self.sender.send_event(CloseConnEvent("request is empty"))
            return
        if request_msg[0] not in (
            constant.MT_MSG,
            constant.MT_CHANNEL,
            constant.MT_CLIENT_EVENT,
            constant.MT_SERVER_EVENT,
        ):
            await self.sender.send_event(CloseConnEvent("Illegal request"))
            return
        correlation_id: int = request_msg[1]
        context: ServerContext = await self.get_context(correlation_id)
        try:
            request: Request = Request.from_msg(request_msg, context=context)
        except Exception as e:
            logger.error(f"{self._conn.peer_tuple} recv bad msg:{request_msg}, error:{e}")
            await self.sender.send_event(CloseConnEvent("protocol error"))
            await self.close_context(context)
            return

        try:
            response, close_context_flag = await self.dispatch(request)
            await self.sender(response)
            if close_context_flag:
                await self.close_context(context)
        except (IOError, BrokenPipeError, ConnectionError, CloseConnException):
            await self.close_context(context)
        except Exception as e:
            logging.exception(f"raw_request handle error e, {e}")
            if request.msg_type != constant.MT_SERVER_EVENT:
                # If an event is received from the client in response,
                # it should not respond even if there is an error
                await self.sender.send_event(ServerErrorEvent(str(e)))
            await self.close_context(context)

    async def dispatch(self, request: Request) -> Tuple[Optional[Response], bool]:
        """recv request, processor request and dispatch request by request msg type"""
        # gen response object
        response: "Response" = Response(context=request.context)

        if self._processor:
            # If the processor does not pass the check, then an error is thrown and the error is returned directly
            try:
                request = await self._processor.process_request(request)
            except Exception as e:
                if not isinstance(e, BaseRapError):
                    logger.exception(e)
                response.set_exception(e)
                return response, True

        try:
            dispatch_func: DISPATCH_FUNC_TYPE = self.dispatch_func_dict[request.msg_type]
            return await dispatch_func(request, response)
        except BaseRapError as e:
            response.set_exception(e)
            return response, True
        except Exception as e:
            logger.debug(e)
            logger.debug(traceback.format_exc())
            response.set_exception(RpcRunTimeError())
            return response, True

    async def ping_event(self) -> None:
        """send ping event to conn"""
        ping_interval: float = self._ping_sleep_time * self._ping_fail_cnt
        # add jitter time
        ping_interval += min(ping_interval * 0.1, random.randint(5, 15))
        while not self._conn.is_closed():
            diff_time: int = int(time.time()) - self._keepalive_timestamp
            try:
                with Deadline(self._ping_sleep_time):
                    if diff_time > ping_interval:
                        await self.sender.send_event(CloseConnEvent("recv pong timeout"))
                        return
                    else:
                        await self.sender.send_event(PingEvent(""))
                        await self._conn.conn_future
            except asyncio.CancelledError:
                return
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.exception(f"{self._conn} ping event exit.. error:<{e.__class__.__name__}>[{e}]")
                return

    async def channel_handle(self, request: Request, response: Response) -> Tuple[Optional[Response], bool]:
        # declare var
        channel_id: int = request.correlation_id
        life_cycle: str = request.header.get("channel_life_cycle", "error")
        channel: Optional[Channel] = request.context.get_value("_channel", None)
        if life_cycle == constant.MSG:
            # Messages with a life cycle of `msg` in the channel type account for the highest proportion
            if channel is None:
                raise ChannelError("channel not create")
            await channel.queue.put(request)
            return None, False
        elif life_cycle == constant.DECLARE:
            if channel is not None:
                raise ChannelError("channel already create")
            func: Callable = (await self._get_func_from_request(request)).func

            try:
                get_corresponding_channel_class(func)
            except Exception as e:
                raise ChannelError(str(e))

            async def write(body: Any, header: Dict[str, Any], timeout: Optional[int] = None) -> None:
                await self.sender(
                    Response(header=header, body=body, context=request.context),
                    deadline=get_deadline(timeout),
                )

            channel = Channel(channel_id, write, self._conn, func)
            request.context._channel = channel
            request.context.context_channel = channel.get_context_channel()
            request.context.channel_metadata = request.body

            async def recycle_channel_resource() -> None:
                # Recycling Channel Resources
                try:
                    await channel.func_future
                except Exception:
                    pass
                try:
                    await channel.channel_future
                except Exception:
                    pass
                await self.close_context(request.context)

            self.create_future_by_resource(recycle_channel_resource())
            response.header["channel_life_cycle"] = constant.DECLARE
            return response, False
        elif life_cycle == constant.DROP:
            if channel is None:
                raise ChannelError("channel not create")
            else:
                await channel.close()
                return None, False
        else:
            raise ChannelError("channel life cycle error")

    async def msg_handle(self, request: Request, response: Response) -> Tuple[Optional[Response], bool]:
        func_model: FuncModel = await self._get_func_from_request(request)
        # Check param type
        try:
            param_dict: dict = func_model.func_sig.bind(*tuple(), **request.body).arguments
        except TypeError as e:
            raise ParseError(extra_msg=str(e))

        if asyncio.iscoroutinefunction(func_model.func):
            coroutine: Union[Awaitable, Coroutine] = func_model.func(**param_dict)
        else:
            coroutine = get_event_loop().run_in_executor(None, partial(func_model.func, **param_dict))

        # called func
        try:
            deadline_timestamp: int = request.header.get("X-rap-deadline", 0)
            if deadline_timestamp:
                timeout: int = int(time.time() - deadline_timestamp)
            else:
                timeout = self._run_timeout
            result: Any = await asyncio.wait_for(coroutine, timeout)
        except asyncio.TimeoutError:
            result = RpcRunTimeError(f"Call {func_model.func.__name__} timeout")
        except Exception as e:
            result = e

        if isinstance(result, Exception):
            exc, exc_info = parse_error(result)
            response.set_exception(InvokeError(exc, exc_info))
        else:
            response.body = result
        return response, True

    async def event(self, request: Request, response: Response) -> Tuple[Optional[Response], bool]:
        """client event request handle"""
        # rap event handle
        if request.func_name == constant.PING_EVENT:
            if request.msg_type == constant.MT_SERVER_EVENT:
                self._conn.keepalive_timestamp = int(time.time())
                return None, True
            elif request.msg_type == constant.MT_CLIENT_EVENT:
                response.set_event(PingEvent({}))
            else:
                raise TypeError(f"Error msg type, {request.correlation_id}")
        elif request.func_name == constant.DECLARE:
            self._client_info = ImmutableDict(request.body["client_info"])
            self._metadata = request.body["metadata"]
            response.context.transport_metadata = self._metadata
            response.set_event(DeclareEvent({"conn_id": self._conn.conn_id, "server_info": self._server_info}))
            self._conn.keepalive_timestamp = int(time.time())

            ping_future: asyncio.Future = self.create_future_by_resource(self.ping_event())
            self._conn.conn_future.add_done_callback(lambda _: ping_future.cancel())
        elif request.func_name == constant.DROP:
            response.set_event(DropEvent("success"))
        return response, True

    def create_future_by_resource(self, coro: Coroutine) -> asyncio.Future:
        future: asyncio.Future = asyncio.create_task(coro)
        future.add_done_callback(lambda f: self._used_resources.remove(f))
        self._used_resources.add(future)
        return future

    async def await_resource_release(self) -> None:
        return await self._used_resources.wait()
