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
from rap.common.utils import (
    Constant,
    Event,
    as_first_completed,
    check_func_type,
    get_event_loop,
    parse_error,
    response_num_dict,
)
from rap.server.model import Request, Response
from rap.server.processor.base import BaseProcessor
from rap.server.registry import FuncModel
from rap.server.sender import Sender

if TYPE_CHECKING:
    from rap.server.core import Server

__all__ = ["Channel", "Receiver"]


class Channel(BaseChannel):
    def __init__(
        self,
        channel_id: str,
        write: Callable[[Any, Dict[str, Any]], Coroutine[Any, Any, Any]],
        close: Callable[[], None],
        conn: ServerConnection,
        func: Callable[["Channel"], Any],
    ):
        self._func_name: str = func.__name__
        self._close: Callable[[], None] = close
        self._write: Callable[[Any, Dict[str, Any]], Coroutine[Any, Any, Any]] = write
        self._conn: ServerConnection = conn
        self._queue: asyncio.Queue = asyncio.Queue()
        self.channel_id: str = channel_id

        # if conn close, channel future will done and channel not read & write
        self._channel_conn_future: asyncio.Future = asyncio.Future()
        self._conn.conn_future.add_done_callback(lambda f: self.set_finish("connection already close"))

        self._func_future: asyncio.Future = asyncio.ensure_future(self._run_func(func))

    async def _run_func(self, func: Callable) -> None:
        try:
            await func(self)
        finally:
            if not self.is_close:
                await self.close()

    async def receive_request(self, request: Request) -> None:
        assert isinstance(request, Request), TypeError(f"request type must {Request}")
        await self._queue.put(request)

    async def write(self, body: Any) -> None:
        if self.is_close:
            raise ChannelError(f"channel{self.channel_id} is close")
        await self._write(body, {"channel_life_cycle": Constant.MSG})

    async def read(self) -> Response:
        if self.is_close:
            raise ChannelError(f"channel{self.channel_id} is close")
        return await as_first_completed(
            [self._queue.get()],
            not_cancel_future_list=[self._channel_conn_future],
        )

    async def read_body(self) -> Any:
        response: Response = await self.read()
        return response.body

    async def close(self) -> None:
        if self.is_close:
            logging.debug("already close channel %s", self.channel_id)
            return
        self.set_finish(f"channel {self.channel_id} is close")

        if not self._conn.is_closed():
            await self._write(None, {"channel_life_cycle": Constant.DROP})

        # Actively cancel the future may not be successful, such as cancel asyncio.sleep
        if not self._func_future.cancelled():
            self._func_future.cancel()
        if self._func_future.done():
            self._func_future.result()
        self._close()


class Receiver(object):
    def __init__(
        self,
        app: "Server",
        conn: ServerConnection,
        run_timeout: int,
        sender: Sender,
        ping_fail_cnt: int,
        ping_sleep_time: int,
        processor_list: Optional[List[BaseProcessor]] = None,
    ):
        self._app: "Server" = app
        self._conn: ServerConnection = conn
        self._run_timeout: int = run_timeout
        self.sender: Sender = sender
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

    async def dispatch(self, request: Request) -> Optional[Response]:
        response_num: int = response_num_dict.get(request.num, Constant.SERVER_ERROR_RESPONSE)

        # gen response object
        response: "Response" = Response(
            num=response_num,
            msg_id=request.msg_id,
            group=request.group,
            func_name=request.func_name,
            stats=request.stats,
        )
        response.header.update(request.header)

        # check type_id
        if response.num is Constant.SERVER_ERROR_RESPONSE:
            logging.error(f"parse request data: {request} from {self._conn.peer_tuple} error")
            response.set_exception(ServerError("Illegal request"))
            return response

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

        try:
            dispatch_func: Callable = self.dispatch_func_dict[request.num]
            return await dispatch_func(request, response)
        except BaseRapError as e:
            response.set_exception(e)
            return response
        except Exception as e:
            logging.debug(e)
            logging.debug(traceback.format_exc())
            response.set_exception(RpcRunTimeError())
            return response

    async def ping_event(self) -> None:
        ping_interval: float = self._ping_sleep_time * self._ping_fail_cnt
        ping_interval += min(ping_interval * 0.1, 10)
        while not self._conn.is_closed():
            diff_time: int = int(time.time()) - self._keepalive_timestamp
            if diff_time > ping_interval:
                await self.sender.send_event(Event(Constant.EVENT_CLOSE_CONN, "recv pong timeout"))
                if not self._conn.is_closed():
                    self._conn.close()
                return
            else:
                await self.sender.send_event(Event(Constant.PING_EVENT, ""))
                try:
                    await as_first_completed(
                        [asyncio.sleep(self._ping_sleep_time)],
                        not_cancel_future_list=[self._conn.conn_future],
                    )
                except Exception as e:
                    logging.debug(f"{self._conn} ping event exit.. error:{e}")

    async def channel_handle(self, request: Request, response: Response) -> Optional[Response]:
        func: Callable = self.get_func_model(request, Constant.CHANNEL_TYPE).func
        # declare var
        if "channel_id" not in request.header:
            raise ProtocolError("channel request must channel id")

        channel_id: str = request.header.get("channel_id", None)
        if channel_id is None:
            raise ChannelError("error channel id")

        life_cycle: str = request.header.get("channel_life_cycle", "error")
        channel: Optional[Channel] = self._channel_dict.get(channel_id, None)
        if life_cycle == Constant.MSG:
            if channel is None:
                raise ChannelError("channel not create")
            await channel.receive_request(request)
            return None
        elif life_cycle == Constant.DECLARE:
            if channel is not None:
                raise ChannelError("channel already create")

            async def write(body: Any, header: Dict[str, Any]) -> None:
                header["channel_id"] = channel_id
                await self.sender(
                    Response(
                        num=Constant.CHANNEL_RESPONSE,
                        msg_id=-1,
                        group=response.group,
                        func_name=response.func_name,
                        header=header,
                        body=body,
                    )
                )

            def close() -> None:
                del self._channel_dict[channel_id]

            channel = Channel(channel_id, write, close, self._conn, func)
            self._channel_dict[channel_id] = channel

            response.header = {"channel_id": channel_id, "channel_life_cycle": Constant.DECLARE}
            return response
        elif life_cycle == Constant.DROP:
            if channel is None:
                raise ChannelError("channel not create")
            else:
                await channel.close()
                return None
        else:
            raise ChannelError("channel life cycle error")

    def get_func_model(self, request: Request, func_type: str) -> FuncModel:
        func_key: str = self._app.registry.gen_key(request.group, request.func_name, func_type)
        if func_key not in self._app.registry:
            raise FuncNotFoundError(extra_msg=f"name: {request.func_name}")

        func_model: FuncModel = self._app.registry[func_key]
        if func_model.is_private and self._conn.peer_tuple[0] not in ("::1", "127.0.0.1", "localhost"):
            raise FuncNotFoundError(f"No permission to call:`{request.func_name}`, {self._conn.peer_tuple}")
        return func_model

    async def _msg_handle(
        self, request: Request, call_id: int, func: Callable, param: list, default_param: Dict[str, Any]
    ) -> Tuple[int, Any]:
        user_agent: str = request.header.get("user_agent", "None")

        if call_id in self._generator_dict:
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
        else:
            # called func
            if asyncio.iscoroutinefunction(func):
                coroutine: Union[Awaitable, Coroutine] = func(*param, **default_param)
            else:
                coroutine = get_event_loop().run_in_executor(None, partial(func, *param, **default_param))

            try:
                result = await asyncio.wait_for(coroutine, self._run_timeout)
            except asyncio.TimeoutError:
                return call_id, RpcRunTimeError(f"Call {func.__name__} timeout")
            except Exception as e:
                return call_id, e

            if inspect.isgenerator(result) or inspect.isasyncgen(result):
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
        func_model: FuncModel = self.get_func_model(request, Constant.NORMAL_TYPE)

        try:
            call_id: int = request.body["call_id"]
        except KeyError:
            raise ParseError(extra_msg="body miss params")
        param: list = request.body.get("param", [])
        kwarg_param: Dict[str, Any] = request.body.get("default_param", {})

        # param check
        if len(func_model.arg_list) != len(param):
            raise ParseError(
                extra_msg=f"{func_model.func_name} takes {len(func_model.arg_list)}"
                f" positional arguments but {len(param)} were given"
            )
        kwarg_param_set: set = set(kwarg_param.keys())
        fun_kwarg_set: set = set(func_model.kwarg_dict.keys())
        if not kwarg_param_set.issubset(fun_kwarg_set):
            raise ParseError(
                extra_msg=f"{func_model.func_name} can not find default "
                f"param name:{kwarg_param_set.difference(fun_kwarg_set)}"
            )

        try:
            check_func_type(func_model.func, param, kwarg_param)
        except TypeError as e:
            raise ParseError(extra_msg=str(e))

        new_call_id, result = await self._msg_handle(request, call_id, func_model.func, param, kwarg_param)
        response.body = {"call_id": new_call_id}
        if isinstance(result, StopAsyncIteration) or isinstance(result, StopIteration):
            response.header["status_code"] = 301
        elif isinstance(result, Exception):
            exc, exc_info = parse_error(result)
            response.body["exc_info"] = exc_info  # type: ignore
            if request.header.get("user_agent") == Constant.USER_AGENT:
                response.body["exc"] = exc  # type: ignore
        else:
            response.body["result"] = result
            if not is_type(func_model.return_type, type(result)):
                logging.warning(
                    f"{func_model.func} return type is {func_model.return_type}, but result type is {type(result)}"
                )
                response.header["status_code"] = 302
        return response

    async def event(self, request: Request, response: Response) -> None:
        if request.func_name == Constant.PONG_EVENT:
            self._keepalive_timestamp = int(time.time())
        return None

    def __del__(self) -> None:
        if self._ping_pong_future and not self._ping_pong_future.done() and self._ping_pong_future.cancelled():
            self._ping_pong_future.cancel()
