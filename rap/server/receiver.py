import asyncio
import inspect
import logging
import random
import time
import traceback
from functools import partial
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
    Union,
)

from rap.common.asyncio_helper import Deadline, get_event_loop
from rap.common.conn import ServerConnection
from rap.common.event import CloseConnEvent, DeclareEvent, DropEvent, PingEvent, PongEvent
from rap.common.exceptions import (
    BaseRapError,
    ChannelError,
    FuncNotFoundError,
    ParseError,
    ProtocolError,
    RpcRunTimeError,
    ServerError,
)
from rap.common.state import State
from rap.common.types import is_type
from rap.common.utils import Constant, param_handle, parse_error, response_num_dict
from rap.server.channel import Channel
from rap.server.model import Request, Response
from rap.server.plugin.processor.base import BaseProcessor
from rap.server.registry import FuncModel
from rap.server.sender import Sender

if TYPE_CHECKING:
    from rap.server.core import Server

__all__ = ["Receiver"]
logger: logging.Logger = logging.getLogger(__name__)


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
        self._state_dict: Dict[str, State] = {}
        self._ping_sleep_time: int = ping_sleep_time
        self._ping_fail_cnt: int = ping_fail_cnt
        self._processor_list: Optional[List[BaseProcessor]] = processor_list
        self._call_func_permission_fn: Callable[[Request], Awaitable[FuncModel]] = (
            call_func_permission_fn if call_func_permission_fn else self._default_call_fun_permission_fn
        )

        self.dispatch_func_dict: Dict[int, Callable] = {
            Constant.CLIENT_EVENT: self.event,
            Constant.MSG_REQUEST: self.msg_handle,
            Constant.CHANNEL_REQUEST: self.channel_handle,
        }
        # now one conn one Request object
        self._ping_pong_future: Optional[asyncio.Future] = None
        self._keepalive_timestamp: int = int(time.time())
        self._generator_dict: Dict[int, Union[Generator, AsyncGenerator]] = {}
        self._channel_dict: Dict[str, Channel] = {}

    async def _default_call_fun_permission_fn(self, request: Request) -> FuncModel:
        func_model: FuncModel = self._app.registry.get_func_model(
            request, Constant.NORMAL_TYPE if request.msg_type == Constant.MSG_REQUEST else Constant.CHANNEL_TYPE
        )

        if func_model.is_private and request.conn.peer_tuple[0] not in ("::1", "127.0.0.1", "localhost"):
            raise FuncNotFoundError(f"No permission to call:`{request.func_name}`")
        return func_model

    async def dispatch(self, request: Request) -> Optional[Response]:
        """recv request, processor request and dispatch request by request msg type"""
        response_num: int = response_num_dict.get(request.msg_type, Constant.SERVER_ERROR_RESPONSE)

        if request.msg_type == Constant.CHANNEL_REQUEST:
            correlation_id: str = f"{request.conn.peer_tuple}:{request.correlation_id}"
            state: State = self._state_dict.get(correlation_id, request.state)
            self._state_dict[correlation_id] = state
            request.state = state

        # gen response object
        response: "Response" = Response(
            app=self._app,
            target=request.target,
            msg_type=response_num,
            correlation_id=request.correlation_id,
            state=request.state,
        )
        response.header.update(request.header)

        # check type_id
        if response.msg_type == Constant.SERVER_ERROR_RESPONSE:
            logger.error(f"parse request data: {request} from {self._conn.peer_tuple} error")
            response.set_exception(ServerError("Illegal request"))
            return response

        if self._processor_list:
            try:
                for processor in self._processor_list:
                    request = await processor.process_request(request)
            except Exception as e:
                if not isinstance(e, BaseRapError):
                    logger.exception(e)
                response.set_exception(e)
                return response

        try:
            dispatch_func: Callable = self.dispatch_func_dict[request.msg_type]
            return await dispatch_func(request, response)
        except BaseRapError as e:
            response.set_exception(e)
            return response
        except Exception as e:
            logger.debug(e)
            logger.debug(traceback.format_exc())
            response.set_exception(RpcRunTimeError())
            return response

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

    async def channel_handle(self, request: Request, response: Response) -> Optional[Response]:
        func: Callable = (await self._call_func_permission_fn(request)).func
        # declare var
        channel_id: str = request.correlation_id
        life_cycle: str = request.header.get("channel_life_cycle", "error")
        channel: Optional[Channel] = self._channel_dict.get(channel_id, None)
        if life_cycle == Constant.MSG:
            # Messages with a life cycle of `msg` in the channel type account for the highest proportion
            if channel is None:
                raise ChannelError("channel not create")
            await channel.queue.put(request)
            return None
        elif life_cycle == Constant.DECLARE:
            if channel is not None:
                raise ChannelError("channel already create")

            async def write(body: Any, header: Dict[str, Any]) -> None:
                header["channel_id"] = channel_id
                await self.sender(
                    Response(
                        app=self._app,
                        target=request.target,
                        msg_type=Constant.CHANNEL_RESPONSE,
                        correlation_id=channel_id,
                        header=header,
                        body=body,
                        state=request.state,
                    )
                )

            channel = Channel(channel_id, write, self._conn, func)
            request.state.user_channel = channel.user_channel
            channel.channel_conn_future.add_done_callback(lambda f: self._channel_dict.pop(channel_id, None))
            self._channel_dict[channel_id] = channel

            response.header = {"channel_id": channel_id, "channel_life_cycle": Constant.DECLARE}
            return response
        elif life_cycle == Constant.DROP:
            if channel is None:
                raise ChannelError("channel not create")
            else:
                await channel.close()
                correlation_id: str = f"{request.conn.peer_tuple}:{request.correlation_id}"
                self._state_dict.pop(correlation_id, None)
                self._channel_dict.pop(channel_id, None)
                return None
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
            if user_agent != Constant.USER_AGENT:
                result = ProtocolError(f"{user_agent} not support generator")
            else:
                call_id = id(result)
                self._generator_dict[call_id] = result
                if inspect.isgenerator(result):
                    result = next(result)
                else:
                    result = await result.__anext__()
        return call_id, result

    async def msg_handle(self, request: Request, response: Response) -> Optional[Response]:
        """根据函数类型分发请求，以及会对函数结果进行封装"""
        func_model: FuncModel = await self._call_func_permission_fn(request)

        call_id: int = request.body.get("call_id", -1)
        if call_id in self._generator_dict:
            new_call_id, result = await self._gen_msg_handle(call_id)
        elif call_id == -1:
            new_call_id, result = await self._msg_handle(request, call_id, func_model)
        else:
            raise ProtocolError("Error call id")
        response.body = {"call_id": new_call_id}
        if isinstance(result, StopAsyncIteration) or isinstance(result, StopIteration):
            response.status_code = 301
        elif isinstance(result, Exception):
            exc, exc_info = parse_error(result)
            response.body["exc_info"] = exc_info  # type: ignore
            if request.header.get("user_agent") == Constant.USER_AGENT:
                response.body["exc"] = exc  # type: ignore
        else:
            response.body["result"] = result
            if not is_type(func_model.return_type, type(result)):
                logger.warning(
                    f"{func_model.func} return type is {func_model.return_type}, but result type is {type(result)}"
                )
                response.status_code = 302
        return response

    async def event(self, request: Request, response: Response) -> Optional[Response]:
        """client event request handle"""
        # rap event handle
        if request.func_name == Constant.PONG_EVENT:
            self._keepalive_timestamp = int(time.time())
            return None
        elif request.func_name == Constant.PING_EVENT:
            response.set_event(PongEvent({}))
        elif request.func_name == Constant.DECLARE:
            if request.body.get("server_name") != self._app.server_name:
                response.set_event(CloseConnEvent("error server name"))
            else:
                response.set_event(DeclareEvent({"result": True, "conn_id": self._conn.conn_id}))
                self._keepalive_timestamp = int(time.time())
                self._ping_pong_future = asyncio.ensure_future(self.ping_event())
        elif request.func_name == Constant.DROP:
            self.del_receiver()
            response.set_event(DropEvent("success"))
        response.correlation_id = request.correlation_id
        return response

    def del_receiver(self) -> None:
        if self._ping_pong_future and not self._ping_pong_future.done() and not self._ping_pong_future.cancelled():
            self._ping_pong_future.cancel()
