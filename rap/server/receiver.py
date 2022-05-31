import asyncio
import inspect
import logging
import random
import sys
import time
import traceback
from functools import partial
from types import TracebackType
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncGenerator,
    Awaitable,
    Callable,
    Coroutine,
    Dict,
    Generator,
    List,
    Optional,
    Tuple,
    Type,
    Union,
)

from rap.common.asyncio_helper import Deadline, get_event_loop
from rap.common.conn import ServerConnection
from rap.common.event import CloseConnEvent, DeclareEvent, DropEvent, PingEvent
from rap.common.exceptions import (
    BaseRapError,
    ChannelError,
    FuncNotFoundError,
    ParseError,
    ProtocolError,
    RpcRunTimeError,
    ServerError,
)
from rap.common.types import BASE_MSG_TYPE
from rap.common.utils import constant, param_handle, parse_error, response_num_dict
from rap.server.channel import Channel
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
        self._call_func_permission_fn: Callable[[Request], Awaitable[FuncModel]] = (
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
        self._generator_dict: Dict[int, Union[Generator, AsyncGenerator]] = {}
        self._channel_dict: Dict[int, Channel] = {}

        # processor
        # self._processor_list: Optional[List[BaseProcessor]] = processor_list
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

    async def _default_call_fun_permission_fn(self, request: Request) -> FuncModel:
        func_model: FuncModel = self._app.registry.get_func_model(
            request, constant.NORMAL_TYPE if request.msg_type == constant.MSG_REQUEST else constant.CHANNEL_TYPE
        )

        if func_model.is_private and request.context.conn.peer_tuple[0] not in ("::1", "127.0.0.1", "localhost"):
            raise FuncNotFoundError(f"No permission to call:`{request.func_name}`")
        return func_model

    async def close_context(
        self,
        correlation_id: int,
        exc: Optional[Exception] = None,
    ):
        context: Optional[ServerContext] = self.context_dict.pop(correlation_id, None)
        if context:
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
        context: Optional[ServerContext] = self.context_dict.get(correlation_id, None)
        if not context:
            # create context
            context = ServerContext()
            context.app = self  # type: ignore
            context.conn = self._conn
            context.correlation_id = correlation_id
            for on_context_enter in self.on_context_enter_processor_list:
                try:
                    await on_context_enter(context)
                except Exception as e:
                    logger.exception(f"on_context_enter error:{e}")
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
        except Exception as e:
            logging.exception(f"raw_request handle error e, {e}")
            if request.msg_type != constant.SERVER_EVENT:
                # If an event is received from the client in response,
                # it should not respond even if there is an error
                await self.sender.response_exc(ServerError(str(e)), context)
            await self.close_context(correlation_id, e)

    async def dispatch(self, request: Request) -> Tuple[Optional[Response], bool]:
        """recv request, processor request and dispatch request by request msg type"""
        response_num: int = response_num_dict.get(request.msg_type, constant.SERVER_ERROR_RESPONSE)
        if request.msg_type == constant.CHANNEL_REQUEST:
            self.context_dict[request.correlation_id] = request.context

        # gen response object
        response: "Response" = Response(msg_type=response_num, context=request.context)
        if "request_id" in request.header:
            response.header["request_id"] = request.header["request_id"]
        # response.header.update(request.header)

        # check type_id
        if response.msg_type == constant.SERVER_ERROR_RESPONSE:
            logger.error(f"parse request data: {request} from {self._conn.peer_tuple} error")
            response.set_exception(ServerError("Illegal request"))
            return response, True

        if self.process_request_processor_list:
            try:
                for process_request in self.process_request_processor_list:
                    request = await process_request(request)
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
            func: Callable = (await self._call_func_permission_fn(request)).func

            async def write(body: Any, header: Dict[str, Any]) -> None:
                await self.sender(
                    Response(msg_type=constant.CHANNEL_RESPONSE, header=header, body=body, context=request.context)
                )

            channel = Channel(channel_id, write, self._conn, func)
            request.context.user_channel = channel.user_channel
            channel.channel_conn_future.add_done_callback(lambda f: self._channel_dict.pop(channel_id, None))
            self._channel_dict[channel_id] = channel

            response.header["channel_life_cycle"] = constant.DECLARE
            return response, False
        elif life_cycle == constant.DROP:
            if channel is None:
                raise ChannelError("channel not create")
            else:
                await channel.close()
                await self.close_context(channel_id)
                return None, False
        else:
            raise ChannelError("channel life cycle error")

    async def _gen_msg_handle(self, call_id: int) -> Tuple[int, Any]:
        # run generator func next
        try:
            result: Any = self._generator_dict[call_id]
            if inspect.isgenerator(result):
                result = next(result)
            elif inspect.isasyncgen(result):
                result = await result.__anext__()
        except (StopAsyncIteration, StopIteration) as e:
            del self._generator_dict[call_id]
            result = e
        return call_id, result

    async def _msg_handle(self, request: Request, call_id: int, func_model: FuncModel) -> Tuple[int, Exception]:
        """fun call handle"""
        param: list = request.body.get("param", [])

        # Check param type
        try:
            param_tuple: tuple = param_handle(func_model.func_sig, param, {})
        except TypeError as e:
            raise ParseError(extra_msg=str(e))

        # called func
        if asyncio.iscoroutinefunction(func_model.func):
            coroutine: Union[Awaitable, Coroutine] = func_model.func(*param_tuple)
        else:
            coroutine = get_event_loop().run_in_executor(None, partial(func_model.func, *param_tuple))

        try:
            deadline_timestamp: int = request.header.get("X-rap-deadline", 0)
            if deadline_timestamp:
                timeout: int = int(time.time() - deadline_timestamp)
            else:
                timeout = self._run_timeout
            result: Any = await asyncio.wait_for(coroutine, timeout)
        except asyncio.TimeoutError:
            return call_id, RpcRunTimeError(f"Call {func_model.func.__name__} timeout")
        except Exception as e:
            return call_id, e

        # generator fun support
        if inspect.isgenerator(result) or inspect.isasyncgen(result):
            user_agent: str = request.header.get("user_agent", "None")
            if user_agent != constant.USER_AGENT:
                result = ProtocolError(f"{user_agent} not support generator")
            else:
                call_id = id(result)
                self._generator_dict[call_id] = result
                if inspect.isgenerator(result):
                    result = next(result)
                else:
                    result = await result.__anext__()  # type: ignore
        return call_id, result

    async def msg_handle(self, request: Request, response: Response) -> Tuple[Optional[Response], bool]:
        """根据函数类型分发请求，以及会对函数结果进行封装"""
        func_model: FuncModel = await self._call_func_permission_fn(request)

        close_context_flag: bool = False
        call_id: int = request.body.get("call_id", -1)
        if call_id in self._generator_dict:
            new_call_id, result = await self._gen_msg_handle(call_id)
        elif call_id == -1:
            new_call_id, result = await self._msg_handle(request, call_id, func_model)
            close_context_flag = True
        else:
            raise ProtocolError("Error call id")
        response.body = {"call_id": new_call_id}
        if isinstance(result, StopAsyncIteration) or isinstance(result, StopIteration):
            response.status_code = 301
            close_context_flag = True
        elif isinstance(result, Exception):
            exc, exc_info = parse_error(result)
            response.body["exc_info"] = exc_info  # type: ignore
            if request.header.get("user_agent") == constant.USER_AGENT:
                response.body["exc"] = exc  # type: ignore
            close_context_flag = True
        else:
            response.body["result"] = result
            # if not is_type(func_model.return_type, type(result)):
            #     logger.warning(
            #         f"{func_model.func} return type is {func_model.return_type}, but result type is {type(result)}"
            #     )
            #     response.status_code = 302
        return response, close_context_flag

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
                response.set_event(DeclareEvent({"result": True, "conn_id": self._conn.conn_id}))
                self._conn.keepalive_timestamp = int(time.time())
                self._conn.ping_future = asyncio.ensure_future(self.ping_event())
        elif request.func_name == constant.DROP:
            response.set_event(DropEvent("success"))
        return response, True
