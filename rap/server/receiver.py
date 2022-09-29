import asyncio
import logging
import random
import sys
import time
import traceback
from functools import partial
from types import TracebackType
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Coroutine, Dict, List, Optional, Tuple, Type, Union

from rap.common.asyncio_helper import Deadline, SetEvent, get_deadline, get_event_loop
from rap.common.conn import CloseConnException, ServerConnection
from rap.common.event import CloseConnEvent, DeclareEvent, DropEvent, PingEvent
from rap.common.exceptions import (
    BaseRapError,
    ChannelError,
    FuncNotFoundError,
    InvokeError,
    ParseError,
    RpcRunTimeError,
    ServerError,
)
from rap.common.types import BASE_MSG_TYPE
from rap.common.utils import InmutableDict, constant, param_handle, parse_error, response_num_dict
from rap.server.channel import Channel, get_corresponding_channel_class
from rap.server.model import Request, Response, ServerContext
from rap.server.plugin.processor.base import BaseProcessor, belong_to_base_method
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
        call_func_permission_fn: Optional[Callable[[Request], Awaitable[FuncModel]]] = None,
        processor_list: Optional[List[BaseProcessor]] = None,
    ):
        """Receive and process messages from the client, and execute different logics according to the message type
        :param app: server
        :param conn: server conn
        :param run_timeout: Maximum execution time per call
        :param sender: Send response data to the client
        :param ping_fail_cnt: When ping fails continuously and exceeds this value, conn will be disconnected
        :param ping_sleep_time: ping message interval time
        :param call_func_permission_fn: Check the permission to call the private function
        :param processor_list: processor list
        """
        self._app: "Server" = app
        self._conn: ServerConnection = conn
        self._run_timeout: int = run_timeout
        self.sender: Sender = sender
        self.context_dict: Dict[int, ServerContext] = {}
        self._ping_sleep_time: int = ping_sleep_time
        self._ping_fail_cnt: int = ping_fail_cnt
        self._get_func_from_request: Callable[[Request], Awaitable[FuncModel]] = (
            call_func_permission_fn if call_func_permission_fn else self._default_call_fun_permission_fn
        )

        self.dispatch_func_dict: Dict[int, DISPATCH_FUNC_TYPE] = {
            constant.CLIENT_EVENT: self.event,
            constant.SERVER_EVENT: self.event,
            constant.MSG_REQUEST: self.msg_handle,
            constant.CHANNEL_REQUEST: self.channel_handle,
        }
        # now one conn one Request object
        self._keepalive_timestamp: int = int(time.time())
        self._channel_dict: Dict[int, Channel] = {}

        # processor
        processor_list = processor_list or []
        self.on_context_enter_processor_list: List[Callable] = [
            i.on_context_enter for i in processor_list if not belong_to_base_method(i.on_context_enter)
        ]
        self.on_context_exit_processor_list: List[Callable] = [
            i.on_context_exit for i in processor_list if not belong_to_base_method(i.on_context_exit)
        ]
        self.process_request_processor_list: List[Callable] = [
            i.process_request for i in processor_list if not belong_to_base_method(i.process_request)
        ]
        # Store resources that cannot be controlled by the current concurrent process
        self._used_resources: SetEvent = SetEvent()
        self._server_info: InmutableDict = InmutableDict(
            host=self._conn.peer_tuple,
            version=constant.VERSION,
            user_agent=constant.USER_AGENT,
        )
        self._client_info: InmutableDict = InmutableDict()

    async def _default_call_fun_permission_fn(self, request: Request) -> FuncModel:
        func_key: str = self._app.registry.gen_key(
            request.group,
            request.func_name,
            constant.NORMAL_TYPE if request.msg_type == constant.MSG_REQUEST else constant.CHANNEL_TYPE,
        )
        if func_key not in self._app.registry.func_dict:
            raise FuncNotFoundError(extra_msg=f"name: {request.func_name}")
        func_model: FuncModel = self._app.registry.func_dict[func_key]
        if func_model.is_private and request.context.conn.peer_tuple[0] not in ("::1", "127.0.0.1", "localhost"):
            raise FuncNotFoundError(f"No permission to call:`{request.func_name}`")
        return func_model

    async def create_context(self, correlation_id: int) -> ServerContext:
        context: Optional[ServerContext] = self.context_dict.get(correlation_id, None)
        if not context:
            # create context
            context = ServerContext()
            context.app = self  # type: ignore
            context.conn = self._conn
            context.correlation_id = correlation_id
            context.server_info = self._server_info
            context.client_info = self._client_info
            self.context_dict[correlation_id] = context
            logger.debug("create %s context", correlation_id)

            for on_context_enter in self.on_context_enter_processor_list:
                try:
                    await on_context_enter(context)
                except Exception as e:
                    logger.exception(f"on_context_enter error:{e}")
        return context

    async def close_context(
        self,
        correlation_id: int,
        exc: Optional[Exception] = None,
    ):
        context: Optional[ServerContext] = self.context_dict.pop(correlation_id, None)
        if context:
            logger.debug("close %s context", correlation_id)
            if exc:
                exc_type: Optional[Type[BaseException]] = None
                exc_val: Optional[BaseException] = None
                exc_tb: Optional[TracebackType] = None
            else:
                exc_type, exc_val, exc_tb = sys.exc_info()
                if exc_val != exc:
                    logger.error(f"context error: {exc} != {exc_val}")
            for on_context_exit in reversed(self.on_context_exit_processor_list):
                try:
                    await on_context_exit(context, exc_type, exc_val, exc_tb)
                except Exception as e:
                    logger.exception(f"on_context_exit error:{e}")
        else:
            logger.error(f"Can not found {correlation_id} context")

    async def __call__(self, request_msg: Optional[BASE_MSG_TYPE]) -> None:
        if request_msg is None:
            await self.sender.send_event(CloseConnEvent("request is empty"))
            return

        correlation_id: int = request_msg[1]
        context: ServerContext = await self.create_context(correlation_id)
        try:
            request: Request = Request.from_msg(request_msg, context=context)
        except Exception as e:
            logger.error(f"{self._conn.peer_tuple} send bad msg:{request_msg}, error:{e}")
            await self.sender.send_event(CloseConnEvent("protocol error"))
            await self.close_context(correlation_id, e)
            return

        try:
            response, close_context_flag = await self.dispatch(request)
            await self.sender(response)
            if close_context_flag:
                await self.close_context(correlation_id)
        except (IOError, BrokenPipeError, ConnectionError, CloseConnException) as e:
            await self.close_context(correlation_id, e)
        except Exception as e:
            logging.exception(f"raw_request handle error e, {e}")
            if request.msg_type != constant.SERVER_EVENT:
                # If an event is received from the client in response,
                # it should not respond even if there is an error
                await self.sender.response_exc(ServerError(str(e)), context)
            await self.close_context(correlation_id, e)

    async def dispatch(self, request: Request) -> Tuple[Optional[Response], bool]:
        """recv request, processor request and dispatch request by request msg type"""
        response_msg_type: int = response_num_dict.get(request.msg_type, constant.SERVER_ERROR_RESPONSE)
        # gen response object
        response: "Response" = Response(msg_type=response_msg_type, context=request.context)

        if self.process_request_processor_list:
            # If the processor does not pass the check, then an error is thrown and the error is returned directly
            try:
                for process_request in self.process_request_processor_list:
                    request = await process_request(request)
            except Exception as e:
                if not isinstance(e, BaseRapError):
                    logger.exception(e)
                response.set_exception(e)
                return response, True

        # check type_id
        if response.msg_type == constant.SERVER_ERROR_RESPONSE:
            logger.error(f"parse request data: {request} from {self._conn.peer_tuple} error")
            response.set_exception(ServerError("Illegal request"))
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
        channel: Optional[Channel] = self._channel_dict.get(channel_id, None)
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
                    Response(msg_type=constant.CHANNEL_RESPONSE, header=header, body=body, context=request.context),
                    deadline=get_deadline(timeout),
                )

            channel = Channel(channel_id, write, self._conn, func)
            self._channel_dict[channel_id] = channel
            request.context.context_channel = channel.get_context_channel()

            async def wait_channel_close() -> None:
                # Recycling Channel Resources
                try:
                    await channel.func_future
                except Exception:
                    pass
                try:
                    await channel.channel_conn_future
                except Exception:
                    pass
                self._channel_dict.pop(channel_id, None)
                await self.close_context(channel_id)

            self.create_future_by_resource(wait_channel_close())
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
            param_tuple: tuple = param_handle(func_model.func_sig, request.body, {})
        except TypeError as e:
            raise ParseError(extra_msg=str(e))

        if asyncio.iscoroutinefunction(func_model.func):
            coroutine: Union[Awaitable, Coroutine] = func_model.func(*param_tuple)
        else:
            coroutine = get_event_loop().run_in_executor(None, partial(func_model.func, *param_tuple))

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
            if request.msg_type == constant.SERVER_EVENT:
                self._conn.keepalive_timestamp = int(time.time())
                return None, True
            elif request.msg_type == constant.CLIENT_EVENT:
                response.set_event(PingEvent({}))
            else:
                raise TypeError(f"Error msg type, {request.correlation_id}")
        elif request.func_name == constant.DECLARE:
            if request.body.get("server_name") != self._app.server_name:
                response.set_server_event(CloseConnEvent("error server name"))
            else:
                self._client_info = InmutableDict(request.body["client_info"])
                response.set_event(
                    DeclareEvent({"result": True, "conn_id": self._conn.conn_id, "server_info": self._server_info})
                )
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
