import asyncio
import inspect
import logging
import time
from typing import TYPE_CHECKING, Any, Callable, Coroutine, Dict, Generator, List, Optional, Tuple

from rap.common.channel import BaseChannel
from rap.common.conn import ServerConnection
from rap.common.exceptions import (
    ChannelError,
    FuncNotFoundError,
    ParseError,
    ProtocolError,
    RpcRunTimeError,
    ServerError,
)
from rap.common.utlis import MISS_OBJECT, Constant, Event, get_event_loop, parse_error, response_num_dict
from rap.server.model import RequestModel, ResponseModel
from rap.server.processor.base import BaseProcessor
from rap.server.registry import FuncModel
from rap.server.response import Response

if TYPE_CHECKING:
    from rap.server import Server

__all__ = ["Channel", "Request"]


class Channel(BaseChannel):
    def __init__(
        self,
        channel_id: str,
        func_name: str,
        write: Callable[[ResponseModel], Coroutine[Any, Any, Any]],
        close: Callable,
        conn: ServerConnection,
    ):
        self._func_name: str = func_name
        self._close: Callable = close
        self._write: Callable[[ResponseModel], Coroutine[Any, Any, Any]] = write
        self._conn: ServerConnection = conn
        self.queue: asyncio.Queue = asyncio.Queue()
        self.channel_id: str = channel_id
        self._is_close: bool = False
        self.future: Optional[asyncio.Future] = None

    async def write(self, body: Any):
        if self.is_close:
            raise ChannelError(f"channel{self.channel_id} is close")
        response: "ResponseModel" = ResponseModel(
            num=Constant.CHANNEL_RESPONSE,
            msg_id=-1,
            func_name=self._func_name,
            header={"channel_id": self.channel_id, "channel_life_cycle": Constant.MSG},
            body=body,
        )
        await self._write(response)

    async def read(self) -> ResponseModel:
        if self.is_close:
            raise ChannelError(f"channel{self.channel_id} is close")
        return await self.queue.get()

    async def read_body(self) -> Any:
        response: ResponseModel = await self.read()
        return response.body

    async def close(self):
        if self._is_close:
            logging.debug("already close channel %s", self.channel_id)
            return
        self._is_close = True
        if not self._conn.is_closed():
            response: "ResponseModel" = ResponseModel(
                num=Constant.CHANNEL_RESPONSE,
                msg_id=-1,
                func_name=self._func_name,
                header={"channel_id": self.channel_id, "channel_life_cycle": Constant.DROP},
            )
            await self._write(response)
        if not self.future.cancelled():
            self.future.cancel()
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
        self._generator_dict: Dict[int, Generator] = {}
        self._channel_dict: Dict[str, Channel] = {}

    async def dispatch(self, request: RequestModel) -> Optional[ResponseModel]:
        response_num: int = response_num_dict.get(request.num, Constant.SERVER_ERROR_RESPONSE)

        # gen response object
        response: "ResponseModel" = ResponseModel(
            num=response_num,
            func_name=request.func_name,
            msg_id=request.msg_id,
            stats=request.stats,
        )
        response.header.update(request.header)

        try:
            for processor in self._processor_list:
                request = await processor.process_request(request)
        except Exception as e:
            response.body = e
            return response

        # check type_id
        if response.num is Constant.SERVER_ERROR_RESPONSE:
            logging.error(f"parse request data: {request} from {self._conn.peer_tuple} error")
            response.body = ServerError("Illegal request")
            return response

        try:
            dispatch_func: Callable = self.dispatch_func_dict[request.num]
            return await dispatch_func(request, response)
        except Exception as e:
            response.body = RpcRunTimeError(str(e))
            return response

    async def ping_event(self):
        while not self._conn.is_closed():
            diff_time: int = int(time.time()) - self._keepalive_timestamp
            if diff_time > (self._ping_sleep_time * self._ping_fail_cnt) + 10:
                await self._response(
                    ResponseModel(Constant.SERVER_EVENT, body=Event(Constant.EVENT_CLOSE_CONN, "recv pong timeout"))
                )
                if not self._conn.is_closed():
                    self._conn.close()
                break
            else:
                await self._response(ResponseModel(Constant.SERVER_EVENT, body=Event(Constant.PING_EVENT, "")))

                try:
                    # check conn exc
                    await asyncio.wait_for(self._conn.result_future, self._ping_sleep_time)
                except asyncio.TimeoutError:
                    # ignore timeout error
                    pass
                except Exception:
                    break

    async def channel_handle(self, request: RequestModel, response: ResponseModel) -> Optional[ResponseModel]:
        try:
            func: Callable = self.check_func(request, "channel")
        except FuncNotFoundError as e:
            response.body = e
            return response
        # declare var
        channel_id: str = request.header.get("channel_id")
        life_cycle: str = request.header.get("channel_life_cycle", "error")
        func_name: str = func.__name__

        channel: Channel = self._channel_dict.get(channel_id, MISS_OBJECT)
        if life_cycle == Constant.MSG:
            if channel is MISS_OBJECT:
                response.body = ChannelError("channel not create")
                return response
            await channel.queue.put(request)
        elif life_cycle == Constant.DECLARE:
            if channel is not MISS_OBJECT:
                response.body = ChannelError("channel already create")
                return response

            async def write(_response: ResponseModel):
                _response.stats = response.stats
                await self._response(_response)

            def close():
                del self._channel_dict[channel_id]

            channel = Channel(channel_id, func_name, write, close, self._conn)

            async def channel_func():
                try:
                    await func(channel)
                finally:
                    if not channel.is_close:
                        asyncio.ensure_future(channel.close())

            def future_done_callback(future: asyncio.Future):
                logging.debug("channel:%s future status:%s" % (channel_id, future.done()))

            async def add_exc_to_queue(exc):
                await channel.queue.put(exc)

            self._conn.add_listen_exc_func(add_exc_to_queue)
            channel.future = asyncio.ensure_future(channel_func())
            channel.future.add_done_callback(future_done_callback)

            self._channel_dict[channel_id] = channel
            response.header = {"channel_id": channel_id, "channel_life_cycle": Constant.DECLARE}
            return response
        elif life_cycle == Constant.DROP:
            if channel is MISS_OBJECT:
                response.body = ChannelError("channel not create")
                return response
            else:
                await channel.close()
                return
        else:
            response.body = ChannelError("channel life cycle error")
            return response

    def check_func(self, request: RequestModel, type_: str) -> Callable:
        group: str = "default"
        if type(request.body) is dict:
            group = request.body.get("group", "default")

        func_key: str = f"{group}:{type_}:{request.func_name}"

        if func_key not in self._app.registry:
            raise FuncNotFoundError(extra_msg=f"name: {request.func_name}")

        func_model: FuncModel = self._app.registry[func_key]
        if func_model.is_private and self._conn.peer_tuple[0] != "127.0.0.1":
            raise FuncNotFoundError(f"No permission to call:`{request.func_name}`")
        return func_model.func

    async def _msg_handle(self, request: RequestModel, call_id: int, func: Callable, param: str) -> Tuple[int, Any]:
        user_agent: str = request.header.get("user_agent")
        try:
            if call_id in self._generator_dict:
                try:
                    result = self._generator_dict[call_id]
                    if inspect.isgenerator(result):
                        result = next(result)
                    elif inspect.isasyncgen(result):
                        result = await result.__anext__()
                except (StopAsyncIteration, StopIteration) as e:
                    del self._generator_dict[call_id]
                    result = e
            else:
                if asyncio.iscoroutinefunction(func):
                    coroutine: Coroutine = func(*param)
                else:
                    coroutine: Coroutine = get_event_loop().run_in_executor(None, func, *param)

                try:
                    result: Any = await asyncio.wait_for(coroutine, self._run_timeout)
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
            func: Callable = self.check_func(request, "normal")
        except FuncNotFoundError as e:
            response.body = e
            return response

        try:
            call_id: int = request.body["call_id"]
        except KeyError:
            response.body = ParseError(extra_msg="body miss params")
            return response
        param: str = request.body.get("param")
        new_call_id, result = await self._msg_handle(request, call_id, func, param)
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
        return response

    async def event(self, request: RequestModel, response: ResponseModel) -> Optional[ResponseModel]:
        event_name: str = request.body[0]
        if event_name == Constant.PONG_EVENT:
            self._keepalive_timestamp = int(time.time())
            return None

    def __del__(self):
        if self._ping_pong_future and self._ping_pong_future.cancelled():
            self._ping_pong_future.cancel()
