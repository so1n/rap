import asyncio
import inspect
import logging
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

from rap.common.channel import BaseChannel
from rap.common.conn import ServerConnection
from rap.common.exceptions import (
    BaseRapError,
    ChannelError,
    FuncNotFoundError,
    ParseError,
    ProtocolError,
    RpcRunTimeError,
    ServerError,
)
from rap.common.types import is_type
from rap.common.utlis import (
    Constant,
    Event,
    as_first_completed,
    check_func_type,
    get_event_loop,
    parse_error,
    response_num_dict,
)
from rap.server.model import RequestModel, ResponseModel
from rap.server.processor.base import BaseProcessor
from rap.server.registry import FuncModel
from rap.server.response import Response

if TYPE_CHECKING:
    from rap.server.core import Server

__all__ = ["Channel", "Request"]


class Channel(BaseChannel):
    def __init__(
        self,
        channel_id: str,
        func_name: str,
        write: Callable[[Any, Dict[str, Any]], Coroutine[Any, Any, Any]],
        close: Callable,
        conn: ServerConnection,
    ):
        self._func_name: str = func_name
        self._close: Callable = close
        self._write: Callable[[Any, Dict[str, Any]], Coroutine[Any, Any, Any]] = write
        self._conn: ServerConnection = conn
        self.queue: asyncio.Queue = asyncio.Queue()
        self.channel_id: str = channel_id
        self.func_future: Optional[asyncio.Future] = None
        self._channel_future: asyncio.Future = asyncio.Future()

    async def write(self, body: Any) -> None:
        if self.is_close:
            raise ChannelError(f"channel{self.channel_id} is close")
        await self._write(body, {"channel_life_cycle": Constant.MSG})

    async def read(self) -> ResponseModel:
        if self.is_close:
            raise ChannelError(f"channel{self.channel_id} is close")
        return await as_first_completed(
            [self.queue.get()],
            not_cancel_future_list=[self._conn.result_future, self._channel_future],
        )

    async def read_body(self) -> Any:
        response: ResponseModel = await self.read()
        return response.body

    async def close(self) -> None:
        if self.is_close:
            logging.debug("already close channel %s", self.channel_id)
            return
        self.set_finish(f"channel {self.channel_id} is close")
        if not self._conn.is_closed():
            await self._write(None, {"channel_life_cycle": Constant.DROP})
        if self.func_future and not self.func_future.cancelled():
            self.func_future.cancel()
        self._close()


class Request(object):
    def __init__(
        self,
        app: "Server",
        conn: ServerConnection,
        run_timeout: int,
        response: Response,
        ping_fail_cnt: int,
        ping_sleep_time: int,
        processor_list: Optional[List[BaseProcessor]] = None,
    ):
        self._app: "Server" = app
        self._conn: ServerConnection = conn
        self._run_timeout: int = run_timeout
        self._response: Response = response
        self._ping_sleep_time: int = ping_sleep_time
        self._ping_fail_cnt: int = ping_fail_cnt
        self._processor_list: Optional[List[BaseProcessor]] = processor_list

        self.dispatch_func_dict: Dict[int, Callable] = {
            Constant.CLIENT_EVENT: self.event,
            Constant.MSG_REQUEST: self.msg_handle,
            Constant.CHANNEL_REQUEST: self.channel_handle,
        }
        # now one conn one Request object
        self._ping_pong_future: asyncio.Future = asyncio.ensure_future(self.ping_event())
        self._keepalive_timestamp: int = int(time.time())
        self._generator_dict: Dict[int, Union[Generator, AsyncGenerator]] = {}
        self._channel_dict: Dict[str, Channel] = {}

    async def dispatch(self, request: RequestModel) -> Optional[ResponseModel]:
        response_num: int = response_num_dict.get(request.num, Constant.SERVER_ERROR_RESPONSE)

        # gen response object
        response: "ResponseModel" = ResponseModel(
            num=response_num,
            msg_id=request.msg_id,
            group=request.group,
            func_name=request.func_name,
            stats=request.stats,
        )
        response.header.update(request.header)

        if self._processor_list:
            try:
                for processor in self._processor_list:
                    request = await processor.process_request(request)
            except BaseRapError as e:
                response.set_exception(e)
                return response
            except Exception as e:
                logging.exception(e)
                response.set_exception(e)
                return response

        # check type_id
        if response.num is Constant.SERVER_ERROR_RESPONSE:
            logging.error(f"parse request data: {request} from {self._conn.peer_tuple} error")
            response.set_exception(ServerError("Illegal request"))
            return response

        try:
            dispatch_func: Callable = self.dispatch_func_dict[request.num]
            return await dispatch_func(request, response)
        except Exception as e:
            logging.debug(e)
            logging.debug(traceback.print_exc())
            response.set_exception(RpcRunTimeError())
            return response

    async def ping_event(self) -> None:
        while not self._conn.is_closed():
            diff_time: int = int(time.time()) - self._keepalive_timestamp
            if diff_time > (self._ping_sleep_time * self._ping_fail_cnt) + 10:
                await self._response(ResponseModel.from_event(Event(Constant.EVENT_CLOSE_CONN, "recv pong timeout")))
                if not self._conn.is_closed():
                    self._conn.close()
                return
            else:
                await self._response(ResponseModel.from_event(Event(Constant.PING_EVENT, "")))
                try:
                    await as_first_completed(
                        [asyncio.sleep(self._ping_sleep_time)],
                        not_cancel_future_list=[self._conn.result_future],
                    )
                except Exception as e:
                    logging.debug(f"{self._conn} ping event exit.. error:{e}")

    async def channel_handle(self, request: RequestModel, response: ResponseModel) -> Optional[ResponseModel]:
        try:
            func: Callable = self.get_func_model(request, "channel").func
        except FuncNotFoundError as e:
            response.set_exception(e)
            return response
        # declare var
        if "channel_id" not in request.header:
            response.set_exception(ProtocolError("channel request must channel id"))
            return response

        channel_id: str = request.header["channel_id"]
        life_cycle: str = request.header.get("channel_life_cycle", "error")
        func_name: str = func.__name__

        channel: Optional[Channel] = self._channel_dict.get(channel_id, None)
        if life_cycle == Constant.MSG:
            if channel is None:
                response.set_exception(ChannelError("channel not create"))
                return response
            await channel.queue.put(request)
            return None
        elif life_cycle == Constant.DECLARE:
            if channel is not None:
                response.set_exception(ChannelError("channel already create"))
                return response

            async def write(body: Any, header: Dict[str, Any]) -> None:
                header["channel_id"] = channel_id
                _response: "ResponseModel" = ResponseModel(
                    num=Constant.CHANNEL_RESPONSE,
                    msg_id=-1,
                    group=response.group,
                    func_name=response.func_name,
                    header=header,
                    body=body,
                )
                await self._response(_response)

            def close() -> None:
                del self._channel_dict[channel_id]

            channel = Channel(channel_id, func_name, write, close, self._conn)

            async def channel_func() -> None:
                try:
                    await func(channel)
                finally:
                    if channel and not channel.is_close:
                        asyncio.ensure_future(channel.close())

            def future_done_callback(future: asyncio.Future) -> None:
                logging.debug("channel:%s future status:%s" % (channel_id, future.done()))

            channel.func_future = asyncio.ensure_future(channel_func())
            channel.func_future.add_done_callback(future_done_callback)

            self._channel_dict[channel_id] = channel
            response.header = {"channel_id": channel_id, "channel_life_cycle": Constant.DECLARE}
            return response
        elif life_cycle == Constant.DROP:
            if channel is None:
                response.set_exception(ChannelError("channel not create"))
                return response
            else:
                await channel.close()
                return None
        else:
            response.set_exception(ChannelError("channel life cycle error"))
            return response

    def get_func_model(self, request: RequestModel, type_: str) -> FuncModel:
        func_key: str = self._app.registry.gen_key(request.group, request.func_name, type_)
        if func_key not in self._app.registry:
            raise FuncNotFoundError(extra_msg=f"name: {request.func_name}")

        func_model: FuncModel = self._app.registry[func_key]
        if func_model.is_private and self._conn.peer_tuple and self._conn.peer_tuple[0] != "127.0.0.1":
            raise FuncNotFoundError(f"No permission to call:`{request.func_name}`")
        return func_model

    async def _msg_handle(
        self, request: RequestModel, call_id: int, func: Callable, param: list, default_param: Dict[str, Any]
    ) -> Tuple[int, Any]:
        user_agent: str = request.header.get("user_agent", "None")
        try:
            if call_id in self._generator_dict:
                try:
                    result: Any = self._generator_dict[call_id]
                    if inspect.isgenerator(result):
                        result = next(result)
                    elif inspect.isasyncgen(result):
                        result = await result.__anext__()
                except (StopAsyncIteration, StopIteration) as e:
                    del self._generator_dict[call_id]
                    result = e
            else:
                if asyncio.iscoroutinefunction(func):
                    coroutine: Union[Awaitable, Coroutine] = func(*param, **default_param)
                else:
                    coroutine = get_event_loop().run_in_executor(None, partial(func, *param, **default_param))

                try:
                    result = await asyncio.wait_for(coroutine, self._run_timeout)
                except asyncio.TimeoutError:
                    return call_id, RpcRunTimeError(f"Call {func.__name__} timeout")
                except Exception as e:
                    raise e

                if inspect.isgenerator(result):
                    if user_agent != Constant.USER_AGENT:
                        result = ProtocolError(f"{user_agent} not support generator")
                    else:
                        call_id = id(result)
                        self._generator_dict[call_id] = result
                        result = next(result)
                elif inspect.isasyncgen(result):
                    if user_agent != Constant.USER_AGENT:
                        result = ProtocolError(f"{user_agent} not support generator")
                    else:
                        call_id = id(result)
                        self._generator_dict[call_id] = result
                        result = await result.__anext__()
        except Exception as e:
            result = e
        return call_id, result

    async def msg_handle(self, request: RequestModel, response: ResponseModel) -> Optional[ResponseModel]:
        try:
            func_model: FuncModel = self.get_func_model(request, "normal")
        except FuncNotFoundError as e:
            response.set_exception(e)
            return response

        try:
            call_id: int = request.body["call_id"]
        except KeyError:
            response.set_exception(ParseError(extra_msg="body miss params"))
            return response
        param: list = request.body.get("param")
        kwarg_param: Dict[str, Any] = request.body.get("default_param", {})

        # param check
        if len(func_model.arg_list) != len(param):
            response.set_exception(
                ParseError(
                    extra_msg=f"{func_model.name} takes {len(func_model.arg_list)}"
                    f" positional arguments but {len(param)} were given"
                )
            )
            return response
        kwarg_param_set: set = set(kwarg_param.keys())
        fun_kwarg_set: set = set(func_model.kwarg_dict.keys())
        if not kwarg_param_set.issubset(fun_kwarg_set):
            response.set_exception(
                ParseError(
                    extra_msg=f"{func_model.name} can not find default "
                    f"param name:{kwarg_param_set.difference(fun_kwarg_set)}"
                )
            )
            return response

        try:
            check_func_type(func_model.func, param, kwarg_param)
        except TypeError as e:
            response.set_exception(ParseError(extra_msg=str(e)))
            return response

        new_call_id, result = await self._msg_handle(request, call_id, func_model.func, param, kwarg_param)
        response.body = {"call_id": new_call_id}
        if isinstance(result, StopAsyncIteration) or isinstance(result, StopIteration):
            response.header["status_code"] = 301
        elif isinstance(result, Exception):
            exc, exc_info = parse_error(result)
            response.body["exc_info"] = exc_info
            if request.header.get("user_agent") == Constant.USER_AGENT:
                response.body["exc"] = exc
        else:
            response.body["result"] = result
            if func_model.return_type and not is_type(func_model.return_type, type(result)):
                logging.warning(
                    f"{func_model.func} return type is {func_model.return_type}, but result type is {type(result)}"
                )
                response.header["status_code"] = 302
        return response

    async def event(self, request: RequestModel, response: ResponseModel) -> None:
        if request.func_name == Constant.PONG_EVENT:
            self._keepalive_timestamp = int(time.time())
        return None

    def __del__(self) -> None:
        if self._ping_pong_future and not self._ping_pong_future.done() and self._ping_pong_future.cancelled():
            self._ping_pong_future.cancel()
