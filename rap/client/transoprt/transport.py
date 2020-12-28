import asyncio
import logging
import msgpack
import random

from contextvars import ContextVar, Token
from typing import Any, Dict, List, Optional, Type, Tuple

from rap.client.processor.base import BaseProcessor
from rap.client.model import Request, Response
from rap.client.transoprt.channel import Channel
from rap.client.utils import get_rap_exc_dict, raise_rap_error
from rap.common import exceptions as rap_exc
from rap.common.conn import Connection
from rap.common.exceptions import RPCError, ProtocolError
from rap.common.types import BASE_REQUEST_TYPE, BASE_RESPONSE_TYPE
from rap.common.utlis import Constant, Event, MISS_OBJECT, gen_random_str_id


_conn_context: ContextVar[Connection] = ContextVar("conn_context", default=MISS_OBJECT)


class Transport(object):
    def __init__(
            self,
            host_list: List[str],
            timeout: int = 9,
            keep_alive_time: int = 1200,
            ssl_crt_path: Optional[str] = None,
    ):
        self._host_list: List[str] = host_list
        self._conn_dict: Dict[str, Connection] = {}
        self._is_close: bool = True
        self._timeout: int = timeout
        self._ssl_crt_path: str = ssl_crt_path
        self._keep_alive_time: int = keep_alive_time
        self._process_request_list: List = []
        self._process_response_list: List = []

        self._msg_id: int = random.randrange(65535)
        self._rap_exc_dict = get_rap_exc_dict()
        self._listen_future_dict: Dict[str, asyncio.Future] = {}
        self._resp_future_dict: Dict[str, asyncio.Future[Response]] = {}
        self._channel_queue_dict: Dict[str, asyncio.Queue[Response]] = {}

    async def _connect(self, host: str):
        conn = Connection(
            msgpack.Unpacker(raw=False, use_list=False),
            self._timeout,
            ssl_crt_path=self._ssl_crt_path,
        )
        if host in self._conn_dict:
            temp_conn: Connection = self._conn_dict[host]
            if not temp_conn.is_closed():
                await temp_conn.wait_closed()
            del self._conn_dict[host]
        self._conn_dict[host] = conn

        ip, port = host.split(':')
        await conn.connect(ip, int(port))

        if conn.peer in self._listen_future_dict:
            temp_future: 'asyncio.Future' = self._listen_future_dict[conn.peer]
            if not temp_future.cancelled():
                temp_future.cancel()
        self._listen_future_dict[conn.peer] = asyncio.ensure_future(self._listen(conn))

        await self._declare_life_cycle(conn)
        logging.debug(f"Connection to %s...", conn.connection_info)

    async def connect(self):
        if not self._is_close:
            raise ConnectionError(f'{self.__class__.__name__} already connect')
        for host in self._host_list:
            if host in self._conn_dict:
                if not self._conn_dict[host].is_closed():
                    logging.warning(f'{host} already connected')
                else:
                    await self._connect(host)
            else:
                await self._connect(host)
        self._is_close = False

    async def wait_close(self):
        if self._is_close:
            raise RuntimeError(f"{self.__class__.__name__} already closed")
        for host in self._host_list:
            if host not in self._conn_dict:
                logging.warning(f'{host} not init')
            elif self._conn_dict[host].is_closed():
                logging.warning(f'{host} already close')
            else:
                conn: Connection = self._conn_dict[host]
                await self._drop_life_cycle(conn)
                if not self._listen_future_dict[conn.peer].cancelled():
                    self._listen_future_dict[conn.peer].cancel()
                del self._listen_future_dict[conn.peer]
                await conn.await_close()
        self._is_close = True

    async def _listen(self, conn: Connection):
        """listen server msg"""
        logging.debug(f"listen:%s start", conn.peer)
        try:
            while not conn.is_closed():
                await self._read_from_conn(conn)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logging.exception(f"listen status:{self._is_close} error: {e}, close conn:{conn}")
            if not conn.is_closed():
                conn.close()

    async def _read_from_conn(self, conn: Connection):
        """recv server msg handle"""
        try:
            response_msg: Optional[BASE_RESPONSE_TYPE] = await conn.read(self._keep_alive_time)
            logging.debug(f"recv raw data: %s", response_msg)
        except asyncio.TimeoutError as e:
            logging.error(f"recv response from {conn.connection_info} timeout")
            conn.set_reader_exc(e)
            raise e
        except asyncio.CancelledError:
            return
        except Exception as e:
            conn.set_reader_exc(e)
            raise e

        if response_msg is None:
            raise ConnectionError("Connection has been closed")
        # parse response
        try:
            response: Response = Response(*response_msg)
        except ValueError:
            logging.error(f"recv wrong response:{response_msg}")
            return

        for process_response in self._process_response_list:
            await process_response(response)

        resp_future_id: str = f'{conn.peer}:{response.msg_id}'
        channel_id: Optional[str] = response.header.get('channel_id')
        if channel_id and response.method != Constant.CHANNEL:
            raise ProtocolError(f'recv error method:{response.method}')

        # server error response handle
        if response.num == Constant.SERVER_ERROR_RESPONSE:
            status_code: int = response.header.get("status_code", 500)
            exc: Type["rap_exc.BaseRapError"] = self._rap_exc_dict.get(status_code, rap_exc.BaseRapError)
            if channel_id:
                self._channel_queue_dict[channel_id].put_nowait(exc(response.body))
            else:
                self._resp_future_dict[resp_future_id].set_exception(exc(response.body))
            return

        # server event msg handle
        elif response.num == Constant.SERVER_EVENT:
            event, event_info = response.body
            if event == Constant.EVENT_CLOSE_CONN:
                raise RuntimeError(f"recv close conn event, event info:{event_info}")
            elif event == Constant.PING_EVENT:
                request: Request = Request(
                    Constant.CLIENT_EVENT_RESPONSE, '', '', Event(Constant.PONG_EVENT, "").to_tuple()
                )
                self.before_request_handle(request, conn)
                await self.write(request, -1, conn)
                return

        if channel_id:
            self._channel_queue_dict[channel_id].put_nowait(response)
            return

        # set msg to future_dict's `future`
        if resp_future_id not in self._resp_future_dict:
            logging.error(f"recv listen future id: {resp_future_id} error, client not request")
            return
        self._resp_future_dict[resp_future_id].set_result(response)

    ##############
    # life cycle #
    ##############
    async def _declare_life_cycle(self, conn: Connection):
        """send declare msg and init client id"""
        random_id: str = gen_random_str_id()
        request: Request = Request(Constant.DECLARE_REQUEST, '', Constant.ONE_BY_ONE, random_id)
        response = await self._base_request(request, conn)
        if response.num != Constant.DECLARE_RESPONSE and response.body != random_id[::-1]:
            raise RPCError("declare response error")
        logging.info("declare success")

    async def _drop_life_cycle(self, conn: Connection):
        """send drop msg"""
        random_id: str = gen_random_str_id(8)
        request: Request = Request(Constant.DROP_REQUEST, '', Constant.ONE_BY_ONE, random_id)
        response = await self._base_request(request, conn)
        if response.num != Constant.DROP_RESPONSE and response.body != random_id[::-1]:
            logging.warning("drop response error")
        else:
            logging.info("drop response success")

    ####################################
    # base one by one request response #
    ####################################
    async def _base_request(self, request: Request, conn: Connection) -> Response:
        """gen msg id, send and recv response"""
        msg_id: int = self._msg_id + 1
        # Avoid too big numbers
        self._msg_id = msg_id & 65535

        resp_future_id: str = await self.write(request, msg_id, conn)
        try:
            return await self.read(resp_future_id)
        finally:
            if resp_future_id in self._resp_future_dict:
                del self._resp_future_dict[resp_future_id]

    def before_request_handle(self, request: Request, conn: Optional[Connection] = None) -> Connection:
        """check conn and header"""
        if not conn:
            conn = self.now_conn
        if conn.is_closed():
            raise ConnectionError("The connection has been closed, please call connect to create connection")

        def set_header_value(header_key: str, header_Value: Any):
            """set header value"""
            if header_key not in request.header:
                request.header[header_key] = header_Value

        use_session: bool = False
        if _conn_context.get(MISS_OBJECT) is not MISS_OBJECT:
            use_session = True

        set_header_value("version", Constant.VERSION)
        set_header_value("user_agent", Constant.USER_AGENT)
        set_header_value("use_session", use_session)
        return conn

    #######################
    # base write&read api #
    #######################
    async def write(self, request: Request, msg_id: int, conn: Optional[Connection] = None) -> str:
        conn = self.before_request_handle(request, conn)
        for process_request in self._process_request_list:
            await process_request(request)
        request_msg: BASE_REQUEST_TYPE = request.gen_request_msg(msg_id)
        try:
            await conn.write(request_msg)
            logging.debug(f"send:%s to %s", request_msg, conn.connection_info)
        except asyncio.TimeoutError as e:
            logging.error(f"send to %s timeout, drop data:%s", conn.connection_info, request_msg)
            raise e
        except Exception as e:
            raise e

        if 'channel_id' in request.header:
            return request.header['channel_id']
        else:
            resp_future_id: str = f'{conn.peer}:{msg_id}'
            self._resp_future_dict[resp_future_id] = asyncio.Future()
            return resp_future_id

    async def read(self, resp_future_id: str) -> Response:
        try:
            return await asyncio.wait_for(self._resp_future_dict[resp_future_id], self._timeout)
        except asyncio.TimeoutError:
            raise asyncio.TimeoutError(f"msg_id:{resp_future_id} request timeout")

    ######################
    # one by one request #
    ######################
    async def request(
            self, func_name: str, *args, call_id=-1, conn: Optional[Connection] = None, header: Optional[dict] = None
    ) -> Response:
        """msg request handle"""
        request: Request = Request(
            Constant.MSG_REQUEST, func_name, Constant.ONE_BY_ONE, {"call_id": call_id, "param": args}
        )
        if header is not None:
            request.header.update(header)
        response: Response = await self._base_request(request, conn)
        if response.num != Constant.MSG_RESPONSE:
            raise RPCError("request num error")
        if "exc" in response.body:
            if response.header.get("user_agent") == Constant.USER_AGENT:
                raise_rap_error(response.body["exc"], response.body.get("exc_info", ""))
            else:
                raise RuntimeError(response.body.get("ext_info", ""))
        return response

    ############
    # get conn #
    ############
    def get_random_conn(self) -> Connection:
        key: str = random.choice(self._host_list)
        return self._conn_dict[key]

    @property
    def now_conn(self) -> Connection:
        conn: Optional[Connection] = _conn_context.get()
        if conn is MISS_OBJECT:
            conn = self.get_random_conn()
        return conn

    @property
    def session(self) -> 'Session':
        return Session(self)

    def channel(self, func_name: str) -> 'Channel':

        async def create(_channel_id: str):
            self._channel_queue_dict[_channel_id] = asyncio.Queue()

        async def read(_channel_id: str) -> Response:
            return await self._channel_queue_dict[_channel_id].get()

        async def write(request: Request) -> str:
            conn = self.before_request_handle(request)
            return await self.write(request, -1, conn)

        async def close(_call_id: str):
            del self._channel_queue_dict[_call_id]

        return Channel(func_name, self.session, create, read, write, close)

    ##############
    # processor #
    ##############
    def load_processor(self, processor_list: List[BaseProcessor]):
        for middleware in processor_list:
            self._process_request_list.append(middleware.process_request)
            self._process_response_list.append(middleware.process_response)


class Session(object):
    def __init__(self, transport: 'Transport'):
        self._transport: 'Transport' = transport
        self._token: Optional[Token] = None

    async def __aenter__(self) -> 'Session':
        self.create()
        return self

    async def __aexit__(self, *args: Tuple):
        self.close()

    def create(self):
        self._token: Token = _conn_context.set(self._transport.get_random_conn())

    def close(self):
        _conn_context.reset(self._token)

    @property
    def conn(self):
        return self._transport.now_conn

    async def request(self, method: str, *args, call_id=-1) -> Response:
        return await self._transport.request(method, *args, call_id, self.conn)

    async def write(self, request: Request, msg_id: int) -> str:
        return await self._transport.write(request, msg_id, self.conn)

    async def read(self, resp_future_id: str) -> Response:
        return await self._transport.read(resp_future_id)