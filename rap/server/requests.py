import asyncio
import inspect
import logging
import time

from dataclasses import dataclass
from typing import Any, Callable, Coroutine, Dict, Generator, Optional, Tuple

from rap.common.conn import ServerConnection
from rap.common.exceptions import (
    BaseRapError,
    FuncNotFoundError,
    ParseError,
    ProtocolError,
    RpcRunTimeError,
    ServerError
)
from rap.common.utlis import (
    Constant,
    Event,
    MISS_OBJECT,
    get_event_loop,
    parse_error,
    response_num_dict
)
from rap.manager.func_manager import func_manager
from rap.server.response import Response, ResponseModel


@dataclass()
class RequestModel(object):
    num: int
    msg_id: int
    header: dict
    body: Any


class Channel(object):
    def __init__(
            self,
            func_name: str,
            write: Callable[[ResponseModel], Coroutine[Any, Any, Any]],
            close: Callable
    ):
        self._func_name: str = func_name
        self._close: Callable = close
        self._write: Callable[[ResponseModel], Coroutine[Any, Any, Any]] = write
        self.queue: asyncio.Queue = asyncio.Queue()
        self.channel_id: int = id(self)

    async def write(self, body: Any):
        response: "ResponseModel" = ResponseModel(
            num=Constant.MSG_RESPONSE,
            msg_id=-1,
            header={'type': 'channel', 'channel_id': self.channel_id, 'channel_life_cycle': 'msg'},
            body={"call_id": -1, "method_name": self._func_name, "body": body}
        )
        await self._write(response)

    async def read(self) -> ResponseModel:
        return await self.queue.get()

    async def read_body(self) -> Any:
        response: ResponseModel = await self.read()
        return response.body.get('body')

    def close(self):
        self._close()


class Request(object):
    def __init__(self, conn: ServerConnection, run_timeout: int, response: Response):
        self._conn: ServerConnection = conn
        self._run_timeout: int = run_timeout
        self.dispatch_func_dict: Dict[int, Callable] = {
            Constant.DECLARE_REQUEST: self.declare_life_cycle,
        }
        self._response: Response = response

        # now one conn one Request object
        self._is_declare: bool = False
        self._ping_pong_future: Optional[asyncio.Future] = None
        self._keepalive_timestamp: int = int(time.time())
        self._generator_dict: Dict[int, Generator] = {}
        self._channel_dict: Dict[int, Channel] = {}

    async def dispatch(self, request: RequestModel) -> Optional[ResponseModel]:
        if request.num not in self.dispatch_func_dict:
            response_num: int = Constant.SERVER_ERROR_RESPONSE
            content: str = 'life cycle error'
        else:
            response_num: int = response_num_dict.get(request.num, Constant.SERVER_ERROR_RESPONSE)
            content: str = 'request num error'

        response: "ResponseModel" = ResponseModel(num=response_num, msg_id=request.msg_id)
        # check type_id
        if response.num is Constant.SERVER_ERROR_RESPONSE:
            logging.error(f"parse request data: {request} from {request.header['_host']} error")
            response.body = ServerError(content)
            return response
        # check conn_data_model
        if request.num != Constant.DECLARE_REQUEST and not self._is_declare:
            response.body = ProtocolError('Must declare')
            return response

        return await self.real_dispatch(request, response)

    async def real_dispatch(self, request: RequestModel, response: ResponseModel) -> ResponseModel:
        dispatch_func: Callable = self.dispatch_func_dict[request.num]
        return await dispatch_func(request, response)

    async def ping_event(self):
        response: Response = Response(self._conn)
        while not self._conn.is_closed():
            diff_time: int = int(time.time()) - self._keepalive_timestamp
            if diff_time > 130:
                event_resp: ResponseModel = ResponseModel(
                    Constant.SERVER_EVENT, body=Event(Constant.EVENT_CLOSE_CONN, "recv pong timeout")
                )
                await response(event_resp)
                if not self._conn.is_closed():
                    self._conn.close()
                break
            else:
                ping_response: ResponseModel = ResponseModel(Constant.SERVER_EVENT, body=Event(Constant.PING_EVENT, ""))
                await response(ping_response)
                await asyncio.sleep(60)

    async def declare_life_cycle(self, request: RequestModel, response: ResponseModel) -> ResponseModel:
        declare_id: str = request.body.get('declare_id', '')
        if not declare_id:
            response.body = ProtocolError('not found declare id')
        self.dispatch_func_dict = {
            Constant.MSG_REQUEST: self.msg_life_cycle,
            Constant.DROP_REQUEST: self.drop_life_cycle,
            Constant.CLIENT_EVENT_RESPONSE: self.event,
        }
        self._is_declare = True
        self._ping_pong_future = asyncio.ensure_future(self.ping_event())
        response.body = {'declare_id': declare_id[::-1]}
        return response

    async def msg_life_cycle(self, request: RequestModel, response: ResponseModel) -> Optional[ResponseModel]:

        try:
            call_id: int = request.body["call_id"]
            method_name: str = request.body["method_name"]
        except KeyError:
            response.body = ParseError("body miss params")
            return response
        param: str = request.body.get("param")

        # root func only called by local client
        if method_name.startswith("_root_") and request.header["_host"] != "127.0.0.1":
            response.body = FuncNotFoundError(extra_msg=f"func name: {method_name}")
            return response
        func: Optional[Callable] = func_manager.func_dict.get(method_name)
        if not func:
            response.body = FuncNotFoundError(extra_msg=f"func name: {method_name}")
            return response

        if request.header.get('type') == 'channel':
            return await self.channel_handle(request, response, func)

        new_call_id, result = await self.msg_handle(request, call_id, func, param)
        response.body = {"call_id": new_call_id, "method_name": method_name}
        if isinstance(result, StopAsyncIteration) or isinstance(result, StopIteration):
            response.body["result"] = ""
            response.header["status_code"] = 301
        elif isinstance(result, Exception):
            exc, exc_info = parse_error(result)
            response.body['exc_info'] = exc_info
            if request.header.get("user_agent") == Constant.USER_AGENT:
                response.body['exc'] = exc
        else:
            response.body["result"] = result
        return response

    async def drop_life_cycle(self, request: RequestModel, response: ResponseModel) -> ResponseModel:
        call_id = request.body["call_id"]
        if self._ping_pong_future:
            if self._ping_pong_future.cancelled():
                self._ping_pong_future.cancel()
        self.dispatch_func_dict = {
            Constant.DROP_REQUEST: self.drop_life_cycle,
            Constant.CLIENT_EVENT_RESPONSE: self.event,
        }
        response.body = {"call_id": call_id, "result": 1}
        return response

    async def event(self, request: RequestModel, response: ResponseModel) -> Optional[ResponseModel]:
        event_name: str = request.body[0]
        if event_name == Constant.PONG_EVENT:
            self._keepalive_timestamp = int(time.time())
        return None

    async def channel_handle(
            self, request: RequestModel, response: ResponseModel, func: Callable
    ) -> Optional[ResponseModel]:
        channel_id: int = request.header.get('channel_id')
        life_cycle: str = request.header.get("channel_life_cycle")
        func_name: str = func.__name__

        channel: Channel = self._channel_dict.get(channel_id, MISS_OBJECT)
        if life_cycle == 'msg:':
            await channel.queue.put(request)
        elif channel is MISS_OBJECT and life_cycle == 'declare':
            async def write(_response: ResponseModel):
                await self._response(_response)

            def close(_call_id: int):
                del self._channel_dict[_call_id]

            channel = Channel(func_name, write, close)
            asyncio.ensure_future(func(channel))
            channel_id: int = id(channel)
            self._channel_dict[channel_id] = channel
            response.header = {'type': 'channel', 'channel_id': channel_id, 'channel_life_cycle': 'declare'}
            response.body = {"call_id": -1, "method_name": func_name}
            return response
        elif life_cycle == 'drop':
            channel.close()
            response.header = {'type': 'channel', 'channel_id': channel_id, 'channel_life_cycle': 'drop'}
            response.body = {"call_id": -1, "method_name": func_name}
            return response
        else:
            await channel.queue.put(request)

    async def msg_handle(self, request: RequestModel, call_id: int, func: Callable, param: str) -> Tuple[int, Any]:
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
            if isinstance(e, BaseRapError):
                result = e
            else:
                logging.exception(f"run:{func} param:{param} error:{e}.")
                result = RpcRunTimeError("execute func error")
        return call_id, result
