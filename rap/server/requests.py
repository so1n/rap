import asyncio
import inspect
import logging
import time

from typing import Any, Callable, Coroutine, Dict, Generator, List, Optional, Tuple

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
from rap.server.filter_stream.base import BaseFilter
from rap.server.model import RequestModel, ResponseModel
from rap.server.response import Response


class Channel(object):
    def __init__(
            self,
            channel_id: str,
            func_name: str,
            write: Callable[[ResponseModel], Coroutine[Any, Any, Any]],
            close: Callable,
            conn: ServerConnection
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
            raise asyncio.CancelledError(f'channel{self.channel_id} is close')
        response: "ResponseModel" = ResponseModel(
            num=Constant.MSG_RESPONSE,
            msg_id=-1,
            func_name=self._func_name,
            method=Constant.CHANNEL,
            header={'type': 'channel', 'channel_id': self.channel_id, 'channel_life_cycle': 'msg'},
            body=body
        )
        await self._write(response)

    async def read(self) -> ResponseModel:
        if self.is_close:
            raise asyncio.CancelledError(f'channel{self.channel_id} is close')
        return await self.queue.get()

    async def read_body(self) -> Any:
        response: ResponseModel = await self.read()
        return response.body

    @property
    def is_close(self) -> bool:
        return self._is_close or (self._conn and self._conn.is_closed())

    async def close(self):
        if self._is_close:
            logging.debug('already close channel %s', self.channel_id)
            return
        self._is_close = True
        if not self.future.cancelled():
            self.future.cancel()
        response: "ResponseModel" = ResponseModel(
            num=Constant.MSG_RESPONSE,
            msg_id=-1,
            func_name=self._func_name,
            method=Constant.CHANNEL,
            header={'channel_id': self.channel_id, 'channel_life_cycle': 'drop'},
        )
        await self._write(response)
        self._close()


class Request(object):
    def __init__(
            self,
            conn: ServerConnection,
            run_timeout: int,
            response: Response,
            filter_list: Optional[List[BaseFilter]] = None
    ):
        self._conn: ServerConnection = conn
        self._run_timeout: int = run_timeout
        self._response: Response = response
        self._filter_list: Optional[List[BaseFilter]] = filter_list

        self.dispatch_func_dict: Dict[int, Callable] = {
            Constant.DECLARE_REQUEST: self.declare_life_cycle,
        }
        # now one conn one Request object
        self._is_declare: bool = False
        self._ping_pong_future: Optional[asyncio.Future] = None
        self._keepalive_timestamp: int = int(time.time())
        self._generator_dict: Dict[int, Generator] = {}
        self._channel_dict: Dict[str, Channel] = {}

    async def dispatch(self, request: RequestModel) -> Optional[ResponseModel]:
        if request.num not in self.dispatch_func_dict:
            response_num: int = Constant.SERVER_ERROR_RESPONSE
            content: str = 'life cycle error'
        else:
            response_num: int = response_num_dict.get(request.num, Constant.SERVER_ERROR_RESPONSE)
            content: str = 'request num error'

        response: "ResponseModel" = ResponseModel(
            num=response_num,
            func_name=request.func_name,
            method=request.method,
            msg_id=request.msg_id,
            stats=request.stats
        )
        print(response.stats._state)
        try:
            for filter_ in self._filter_list:
                await filter_.process_request(request)
        except Exception as e:
            response.body = e
            return response

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
        while not self._conn.is_closed():
            diff_time: int = int(time.time()) - self._keepalive_timestamp
            if diff_time > 130:
                event_resp: ResponseModel = ResponseModel(
                    Constant.SERVER_EVENT, body=Event(Constant.EVENT_CLOSE_CONN, "recv pong timeout")
                )
                await self._response(event_resp)
                if not self._conn.is_closed():
                    self._conn.close()
                break
            else:
                ping_response: ResponseModel = ResponseModel(Constant.SERVER_EVENT, body=Event(Constant.PING_EVENT, ""))
                await self._response(ping_response)
                await asyncio.sleep(60)

    async def declare_life_cycle(self, request: RequestModel, response: ResponseModel) -> ResponseModel:
        random_id: str = request.body
        if not random_id:
            response.body = ProtocolError('not found declare id')
        self.dispatch_func_dict = {
            Constant.MSG_REQUEST: self.msg_life_cycle,
            Constant.DROP_REQUEST: self.drop_life_cycle,
            Constant.CLIENT_EVENT_RESPONSE: self.event,
        }
        self._is_declare = True
        self._ping_pong_future = asyncio.ensure_future(self.ping_event())
        response.body = random_id[::-1]
        return response

    async def msg_life_cycle(self, request: RequestModel, response: ResponseModel) -> Optional[ResponseModel]:
        # root func only called by local client
        if request.func_name.startswith("_root_") and request.header["_host"] != "127.0.0.1":
            response.body = FuncNotFoundError(extra_msg=f"func name: {request.func_name}")
            return response
        func: Optional[Callable] = func_manager.func_dict.get(request.func_name)
        if not func:
            response.body = FuncNotFoundError(extra_msg=f"func name: {request.func_name}")
            return response

        if request.method == Constant.CHANNEL:
            return await self.channel_handle(request, response, func)
        try:
            call_id: int = request.body["call_id"]
        except KeyError:
            response.body = ParseError("body miss params")
            return response
        param: str = request.body.get("param")
        new_call_id, result = await self.msg_handle(request, call_id, func, param)
        response.body = {"call_id": new_call_id}
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
        random_id: str = request.body
        if self._ping_pong_future:
            if self._ping_pong_future.cancelled():
                self._ping_pong_future.cancel()
        self.dispatch_func_dict = {
            Constant.DROP_REQUEST: self.drop_life_cycle,
            Constant.CLIENT_EVENT_RESPONSE: self.event,
        }
        response.body = random_id[::-1]
        return response

    async def event(self, request: RequestModel, response: ResponseModel) -> Optional[ResponseModel]:
        event_name: str = request.body[0]
        if event_name == Constant.PONG_EVENT:
            self._keepalive_timestamp = int(time.time())
        return None

    async def channel_handle(
            self, request: RequestModel, response: ResponseModel, func: Callable
    ) -> Optional[ResponseModel]:
        channel_id: str = request.header.get('channel_id')
        life_cycle: str = request.header.get("channel_life_cycle")
        func_name: str = func.__name__

        channel: Channel = self._channel_dict.get(channel_id, MISS_OBJECT)
        if life_cycle == 'msg:':
            await channel.queue.put(request)
        elif life_cycle == 'declare':
            if channel is not MISS_OBJECT:
                response: "ResponseModel" = ResponseModel(
                    num=Constant.MSG_RESPONSE,
                    msg_id=-1,
                    func_name=func_name,
                    method=Constant.CHANNEL,
                    header={'channel_id': channel_id, 'channel_life_cycle': 'declare'},
                    body=ProtocolError('channel already create')
                )
                return response

            async def write(_response: ResponseModel):
                await self._response(_response)

            def close():
                del self._channel_dict[channel_id]

            channel = Channel(channel_id, func_name, write, close, self._conn)

            async def channel_func():
                try:
                    await func(channel)
                finally:
                    if not channel.is_close:
                        await channel.close()

            channel.future = asyncio.ensure_future(channel_func())
            self._channel_dict[channel_id] = channel
            response.header = {'channel_id': channel_id, 'channel_life_cycle': 'declare'}
            return response
        elif life_cycle == 'drop':
            await channel.close()
            return
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
