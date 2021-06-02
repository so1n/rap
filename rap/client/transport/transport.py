import asyncio
import logging
import random
import uuid
from contextvars import ContextVar, Token
from copy import deepcopy
from dataclasses import dataclass, field
from types import FunctionType
from typing import Any, Callable, Coroutine, Dict, List, Optional, Sequence, Tuple, Type, Union

from rap.client.model import Request, Response
from rap.client.processor.base import BaseProcessor
from rap.client.transport.channel import Channel
from rap.client.utils import get_exc_status_code_dict, raise_rap_error
from rap.common import exceptions as rap_exc
from rap.common.conn import Connection
from rap.common.exceptions import ChannelError, RPCError, ServerError
from rap.common.state import WindowState
from rap.common.types import BASE_REQUEST_TYPE, BASE_RESPONSE_TYPE
from rap.common.utils import Constant, Event, RapFunc, as_first_completed

_session_context: ContextVar["Optional[Session]"] = ContextVar("session_context", default=None)
__all__ = ["Session", "Transport"]


@dataclass()
class ConnModel(object):
    conn: Connection
    future: asyncio.Future = field(default_factory=asyncio.Future)
    weight: int = 0
    min_weight: int = 0
    probability: float = 1

    def __post_init__(self) -> None:
        self.future.set_result(True)

    def is_closed(self) -> bool:
        return self.conn.is_closed() or self.future.done()

    async def await_close(self) -> None:
        if not self.future.cancelled():
            self.future.cancel()
        if not self.conn.is_closed():
            await self.conn.await_close()


class Transport(object):
    """base client transport, encapsulation of custom transport protocol"""

    def __init__(
        self,
        host_list: List[str],
        timeout: int = 9,
        keep_alive_time: int = 1200,
        ssl_crt_path: Optional[str] = None,
        fuse_window_state: Optional[WindowState] = None,
        fuse_k: float = 2.0,  # google sre default
        fuse_enable_cnt: int = 100,
        fuse_min_probability: float = 0.1,
    ):
        self._host_weight_list: List[str] = host_list
        self._conn_dict: Dict[str, ConnModel] = {}
        self._connected_cnt: int = 0
        self._timeout: int = timeout
        self._ssl_crt_path: Optional[str] = ssl_crt_path
        self._keep_alive_time: int = keep_alive_time
        self._process_request_list: List = []
        self._process_response_list: List = []

        self._msg_id: int = random.randrange(65535)
        self._round_robin_index: int = 0
        self._exc_status_code_dict = get_exc_status_code_dict()
        self._resp_future_dict: Dict[str, asyncio.Future[Response]] = {}
        self._channel_queue_dict: Dict[str, asyncio.Queue[Union[Response, Exception]]] = {}
        self._fuse_enable_cnt: int = fuse_enable_cnt
        self._fuse_window_state: WindowState = fuse_window_state or WindowState(interval=2 * 60)

        def upload_probability(stats_dict: dict) -> None:
            host_weight_list: List[str] = []
            for key, conn_model in self._conn_dict.items():
                peer_tuple: Tuple[str, int] = conn_model.conn.peer_tuple
                total: int = stats_dict.get(f"{peer_tuple}:total", 0)  # type: ignore
                error_cnt: int = stats_dict.get(f"{peer_tuple}:error", 0)  # type: ignore
                conn_model.probability = max(fuse_min_probability, (total - fuse_k * (total - error_cnt)) / (total + 1))
                host_weight_list.extend(
                    [
                        f"{peer_tuple[0]}:{peer_tuple[1]}"
                        for _ in range(int(conn_model.probability * conn_model.weight * 10))
                    ]
                )
            random.shuffle(host_weight_list)
            self._host_weight_list = host_weight_list

        self._fuse_window_state.add_priority_callback(upload_probability)

    async def _connect(self, conn_model: ConnModel) -> None:
        """create conn and listen future by host"""

        def _conn_done(f: asyncio.Future) -> None:
            try:
                key: str = conn_model.conn.connection_info
                if key in self._conn_dict:
                    del self._conn_dict[key]
                    self._host_weight_list.remove(key)
                self._connected_cnt -= 1
            except Exception as e:
                logging.exception(f"close conn error: {e}")

        await conn_model.conn.connect()
        self._connected_cnt += 1
        conn_model.future = asyncio.ensure_future(self._listen(conn_model.conn))
        conn_model.future.add_done_callback(lambda f: _conn_done(f))
        logging.debug(f"Connection to %s...", conn_model.conn.connection_info)

    def add_conn(self, ip: str, port: int, weight: int = 1, min_weight: Optional[int] = None) -> None:
        if not min_weight or min_weight >= 1:
            min_weight = 1
        key: str = f"{ip}:{port}"
        self._host_weight_list.extend([key for _ in range(weight)])
        random.shuffle(self._host_weight_list)

        conn_model: ConnModel = ConnModel(
            Connection(ip, port, self._timeout, ssl_crt_path=self._ssl_crt_path),
            weight=weight,
            min_weight=min_weight,
        )
        self._conn_dict[key] = conn_model
        if not self.is_close:
            asyncio.ensure_future(self._connect(conn_model))

    def remove_conn(self, ip: str, port: int) -> Optional[Coroutine]:
        key: str = f"{ip}:{port}"
        coro: Optional[Coroutine] = None
        if key not in self._conn_dict:
            return coro

        if not self.is_close:
            conn_model: ConnModel = self._conn_dict[key]
            if not conn_model.is_closed():
                conn_model.conn.close()
                coro = conn_model.conn.wait_closed()

        self._host_weight_list = [i for i in self._host_weight_list if i != key]
        del self._conn_dict[key]
        return coro

    async def start(self) -> None:
        """create conn and listen future"""
        if not self.is_close:
            raise ConnectionError(f"{self.__class__.__name__} is running")
        if not self._conn_dict:
            raise RuntimeError("Not add conn, can not start")
        if self._fuse_window_state.is_closed:
            self._fuse_window_state.change_state()
        for key, conn_model in self._conn_dict.items():
            if conn_model.is_closed():
                await self._connect(conn_model)

    @property
    def is_close(self) -> bool:
        return self._connected_cnt == 0

    async def stop(self) -> None:
        """close all conn and cancel future"""
        future_list: List[Coroutine] = []
        for host in deepcopy(self._host_weight_list):
            index: int = host.rfind(":")
            ip: str = host[:index]
            port: int = int(host[index + 1 :])
            coro: Optional[Coroutine] = self.remove_conn(ip, port)
            if coro:
                future_list.append(coro)
        if not self._fuse_window_state.is_closed:
            self._fuse_window_state.close()
        await asyncio.gather(*future_list)

    async def _listen(self, conn: Connection) -> None:
        """listen server msg"""
        logging.debug(f"listen:%s start", conn.peer_tuple)
        try:
            while not conn.is_closed():
                await self._read_from_conn(conn)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            conn.set_reader_exc(e)
            logging.exception(f"listen {conn} error:{e}")
            if not conn.is_closed():
                await conn.await_close()

    async def _read_from_conn(self, conn: Connection) -> None:
        """recv server msg handle"""
        self._fuse_window_state.increment(f"{conn.peer_tuple}:total")
        try:
            response_msg: Optional[BASE_RESPONSE_TYPE] = await conn.read(self._keep_alive_time)
            logging.debug(f"recv raw data: %s", response_msg)
        except asyncio.TimeoutError as e:
            logging.error(f"recv response from {conn.connection_info} timeout")
            raise e
        except asyncio.CancelledError:
            return

        if response_msg is None:
            raise ConnectionError("Connection has been closed")

        # parse response
        try:
            response: Response = Response.from_msg(response_msg)
        except Exception as e:
            logging.error(f"recv wrong response:{response_msg}, ignore error:{e}")
            return

        exc: Optional[Exception] = None
        try:
            for process_response in self._process_response_list:
                response = await process_response(response)
        except Exception as e:
            exc = e

        resp_future_id: str = f"{conn.sock_tuple}:{response.msg_id}"
        channel_id: Optional[str] = response.header.get("channel_id")
        status_code: int = response.header.get("status_code", 500)

        def put_exc_to_receiver(put_exc: Exception) -> None:
            self._fuse_window_state.increment(f"{conn.peer_tuple}:error")
            if channel_id in self._channel_queue_dict:
                self._channel_queue_dict[channel_id].put_nowait(put_exc)
            elif response.msg_id != -1 and resp_future_id in self._resp_future_dict:
                self._resp_future_dict[resp_future_id].set_exception(put_exc)
            elif isinstance(put_exc, rap_exc.ServerError):
                raise put_exc
            else:
                logging.error(f"recv error msg:{response}, ignore")

        if exc:
            put_exc_to_receiver(exc)
            return
        elif response.num == Constant.SERVER_ERROR_RESPONSE or status_code in self._exc_status_code_dict:
            # server error response handle
            exc_class: Type["rap_exc.BaseRapError"] = self._exc_status_code_dict.get(status_code, rap_exc.BaseRapError)
            put_exc_to_receiver(exc_class(response.body))
            return
        elif response.num == Constant.SERVER_EVENT:
            # server event msg handle
            if response.func_name == Constant.EVENT_CLOSE_CONN:
                event_exc: Exception = ConnectionError(f"recv close conn event, event info:{response.body}")
                raise event_exc
            elif response.func_name == Constant.PING_EVENT:
                request: Request = Request.from_event(Event(Constant.PONG_EVENT, ""))
                await self.write(request, -1, conn)
                return
            else:
                logging.error(f"recv error event {response}")
        elif channel_id:
            # put msg to channel
            if channel_id not in self._channel_queue_dict:
                logging.error(f"recv {channel_id} msg, but {channel_id} not create")
            else:
                self._channel_queue_dict[channel_id].put_nowait(response)
            return
        elif resp_future_id in self._resp_future_dict:
            # set msg to future_dict's `future`
            self._resp_future_dict[resp_future_id].set_result(response)
        else:
            logging.error(f"Can' parse response: {response}, ignore")
            return

    ####################################
    # base one by one request response #
    ####################################
    async def _base_request(self, request: Request, session: Optional["Session"] = None) -> Response:
        """gen msg id, send and recv response"""
        msg_id: int = self._msg_id + 1
        # Avoid too big numbers
        self._msg_id = msg_id & 65535

        conn, resp_future_id = await self.write(request, msg_id, session=session)
        try:
            return await self.read(resp_future_id, conn)
        finally:
            if resp_future_id in self._resp_future_dict:
                del self._resp_future_dict[resp_future_id]

    @staticmethod
    def before_write_handle(request: Request) -> None:
        """check and header"""

        def set_header_value(header_key: str, header_value: Any, is_cover: bool = False) -> None:
            """if key not in header, set header value"""
            if is_cover or header_key not in request.header:
                request.header[header_key] = header_value

        set_header_value("version", Constant.VERSION, is_cover=True)
        set_header_value("user_agent", Constant.USER_AGENT, is_cover=True)
        set_header_value("request_id", str(uuid.uuid4()), is_cover=True)

    #######################
    # base write&read api #
    #######################
    async def write(
        self, request: Request, msg_id: int, conn: Optional[Connection] = None, session: Optional["Session"] = None
    ) -> Tuple[Connection, str]:
        """write msg to conn, If it is not a normal request, you need to set msg_id: -1"""
        if not session or not session.id:
            session = _session_context.get(None)
        if session:
            conn = session.conn
            request.header["session_id"] = session.id
        elif conn is None:
            conn = self.get_random_conn()
        request.header["host"] = conn.peer_tuple

        total: int = self._fuse_window_state.get_value(f"{conn.peer_tuple}:total", 0)  # type: ignore
        if total > self._fuse_enable_cnt:
            probability: float = self._fuse_window_state.get_value(f"{conn.peer_tuple}:probability", 0.0)
            if (random.randint(0, 10) / 10) > probability:
                raise ServerError("Service Unavailable")

        async def _write(_request: Request) -> None:
            self.before_write_handle(_request)

            for process_request in self._process_request_list:
                _request = await process_request(_request)

            request_msg: BASE_REQUEST_TYPE = _request.gen_request_msg(msg_id)
            try:
                await conn.write(request_msg)  # type: ignore
            except asyncio.TimeoutError:
                raise asyncio.TimeoutError(
                    f"send to %s timeout, drop data:%s", conn.connection_info, request_msg  # type: ignore
                )
            except Exception as e:
                raise e

        if request.num == Constant.CHANNEL_REQUEST:
            if "channel_id" not in request.header:
                raise ChannelError("not found channel id in header")
            await _write(request)
            return conn, request.header["channel_id"]
        else:
            await _write(request)
            resp_future_id: str = f"{conn.sock_tuple}:{msg_id}"
            self._resp_future_dict[resp_future_id] = asyncio.Future()
            return conn, resp_future_id

    async def read(self, resp_future_id: str, conn: Connection) -> Response:
        """write response msg(except channel response)"""
        try:
            return await as_first_completed(
                [asyncio.wait_for(self._resp_future_dict[resp_future_id], self._timeout)],
                not_cancel_future_list=[conn.conn_future],
            )
        except asyncio.TimeoutError:
            raise asyncio.TimeoutError(f"msg_id:{resp_future_id} request timeout")

    ######################
    # one by one request #
    ######################
    async def request(
        self,
        func_name: str,
        arg_param: Optional[Sequence[Any]] = None,
        kwarg_param: Optional[Dict[str, Any]] = None,
        call_id: Optional[int] = None,
        group: Optional[str] = None,
        header: Optional[dict] = None,
        session: Optional["Session"] = None,
    ) -> Response:
        """msg request handle"""
        group = group or Constant.DEFAULT_GROUP
        call_id = call_id or -1
        arg_param = arg_param or []
        kwarg_param = kwarg_param or {}
        request: Request = Request(
            Constant.MSG_REQUEST,
            func_name,
            {"call_id": call_id, "param": arg_param, "default_param": kwarg_param},
            group=group,
        )
        if header:
            request.header.update(header)
        response: Response = await self._base_request(request, session=session)
        if response.num != Constant.MSG_RESPONSE:
            raise RPCError(f"request num must:{Constant.MSG_RESPONSE} not {response.num}")
        if "exc" in response.body:
            exc_info: str = response.body.get("exc_info", "")
            if response.header.get("user_agent") == Constant.USER_AGENT:
                raise_rap_error(response.body["exc"], exc_info)
            else:
                raise RuntimeError(exc_info)
        return response

    ############
    # get conn #
    ############
    def get_random_conn(self) -> Connection:
        key: str = random.choice(self._host_weight_list)
        return self._conn_dict[key].conn

    def get_round_robin_conn(self) -> Connection:
        self._round_robin_index += 1
        index = self._round_robin_index % (len(self._host_weight_list))
        key: str = self._host_weight_list[index]
        return self._conn_dict[key].conn

    @property
    def session(self) -> "Session":
        return Session(self)

    @staticmethod
    def get_now_session() -> "Optional[Session]":
        return _session_context.get(None)

    def channel(self, func_name: str, group: Optional[str] = None, session: Optional["Session"] = None) -> "Channel":
        async def create(_channel_id: str) -> None:
            self._channel_queue_dict[_channel_id] = asyncio.Queue()

        async def read(_channel_id: str) -> Response:
            result: Union[Response, Exception] = await self._channel_queue_dict[_channel_id].get()
            if isinstance(result, Exception):
                raise result
            return result

        async def write(request: Request, _session: "Session") -> None:
            await self.write(request, -1, session=_session)

        async def close(_call_id: str) -> None:
            del self._channel_queue_dict[_call_id]

        session = session if session else self.session
        return Channel(func_name, session, create, read, write, close, group=group)

    #############
    # processor #
    #############
    def load_processor(self, processor_list: List[BaseProcessor]) -> None:
        for middleware in processor_list:
            self._process_request_list.append(middleware.process_request)
            self._process_response_list.append(middleware.process_response)


class Session(object):
    """
    `Session` uses `contextvar` to enable transport to use only the same conn in session
    """

    def __init__(self, transport: "Transport"):
        self._transport: "Transport" = transport
        self._token: Optional[Token[Optional[Session]]] = None
        self.id: str = ""
        self._conn: Optional[Connection] = None

    async def __aenter__(self) -> "Session":
        self.create()
        return self

    async def __aexit__(self, *args: Tuple) -> None:
        self.close()

    def create(self) -> None:
        self.id = str(uuid.uuid4())
        self._conn = self._transport.get_random_conn()
        self._token = _session_context.set(self)

    def close(self) -> None:
        self.id = ""
        self._conn = None
        if self._token:
            _session_context.reset(self._token)

    @property
    def in_session(self) -> bool:
        return _session_context.get(None) is not None

    @property
    def conn(self) -> Connection:
        if not self._conn:
            raise ConnectionError("Session has not been created")
        return self._conn

    async def request(
        self,
        name: str,
        arg_param: Sequence[Any],
        kwarg_param: Optional[Dict[str, Any]] = None,
        call_id: int = -1,
        header: Optional[dict] = None,
    ) -> Any:
        return await self._transport.request(
            name, arg_param, kwarg_param=kwarg_param, call_id=call_id, header=header, session=self
        )

    async def write(self, request: Request, msg_id: int) -> None:
        await self._transport.write(request, msg_id, session=self)

    async def read(self, resp_future_id: str) -> Response:
        return await self._transport.read(resp_future_id, self.conn)

    async def execute(
        self,
        obj: Union[RapFunc, Callable, Coroutine, str],
        arg_list: Optional[List] = None,
        kwarg_dict: Optional[Dict[str, Any]] = None,
    ) -> Any:
        if isinstance(obj, RapFunc):
            obj._kwargs_param["session"] = self
            return await obj
        # Has been replaced by rap func
        # elif asyncio.iscoroutine(obj):
        #     assert obj.cr_frame.f_locals["self"].transport is self._transport
        #     obj.cr_frame.f_locals["kwargs"]["session"] = self
        #     return await obj
        elif isinstance(obj, FunctionType) and arg_list:
            kwarg_dict = kwarg_dict if kwarg_dict else {}
            response: Response = await self.request(obj.__name__, arg_list, kwarg_param=kwarg_dict)
            return response.body["result"]
        elif isinstance(obj, str) and arg_list:
            kwarg_dict = kwarg_dict if kwarg_dict else {}
            response = await self.request(obj, arg_list, kwarg_param=kwarg_dict)
            return response.body["result"]
        else:
            raise TypeError(f"Not support {type(obj)}, obj type must: {Callable}, {Coroutine}, {str}")
