import asyncio
import logging
import ssl
from types import FunctionType
from typing import Any, Callable, Coroutine, List, Optional, Set, Union

from rap.common.conn import ServerConnection
from rap.common.exceptions import RpcRunTimeError
from rap.common.middleware import BaseMiddleware
from rap.common.types import BASE_REQUEST_TYPE, READER_TYPE, WRITER_TYPE
from rap.common.utlis import Constant, Event
from rap.manager.func_manager import func_manager
from rap.server.middleware.base import BaseConnMiddleware, BaseMsgMiddleware
from rap.server.model import RequestModel, ResponseModel
from rap.server.processor.base import BaseProcessor
from rap.server.requests import Request
from rap.server.response import Response

__all__ = ["Server"]


class Server(object):
    def __init__(
        self,
        host: Union[str, List] = "localhost:9000",
        timeout: int = 9,
        keep_alive: int = 1200,
        run_timeout: int = 9,
        ping_fail_cnt: int = 2,
        ping_sleep_time: int = 60,
        backlog: int = 1024,
        ssl_crt_path: Optional[str] = None,
        ssl_key_path: Optional[str] = None,
        start_event_list: List[Union[Callable, Coroutine]] = None,
        stop_event_list: List[Union[Callable, Coroutine]] = None,
        middleware_list: List[BaseMiddleware] = None,
        processor_list: List[BaseProcessor] = None,
    ):
        if type(host) is str:
            self._host = [host]
        else:
            self._host = host
        self._timeout: int = timeout
        self._run_timeout: int = run_timeout
        self._keep_alive: int = keep_alive
        self._backlog: int = backlog
        self._ping_fail_cnt: int = ping_fail_cnt
        self._ping_sleep_time: int = ping_sleep_time
        self._server_list: List[asyncio.AbstractServer] = []

        self._ssl_context: Optional[ssl.SSLContext] = None
        if ssl_crt_path and ssl_key_path:
            self._ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
            self._ssl_context.check_hostname = False
            self._ssl_context.load_cert_chain(ssl_crt_path, ssl_key_path)

        self._start_event_list: List[Union[Callable, Coroutine]] = []
        self._stop_event_list: List[Union[Callable, Coroutine]] = []
        self._middleware_list: List[BaseMiddleware] = []
        self._processor_list: List[BaseProcessor] = []
        self._depend_set: Set[Any] = set()  # Check whether any components have been re-introduced

        if start_event_list:
            self.load_start_event(start_event_list)
        if stop_event_list:
            self.load_stop_event(stop_event_list)
        if middleware_list:
            self.load_middleware(middleware_list)
        if processor_list:
            self.load_processor(processor_list)

    def _load_event(self, event_list: List[Union[Callable, Coroutine]], event: Union[Callable, Coroutine]):
        if not (isinstance(object, FunctionType) or asyncio.iscoroutine(event)):
            raise ImportError(f"{event} must be fun or coroutine")

        if event not in self._depend_set:
            self._depend_set.add(event)
        else:
            raise ImportError(f"{event} event already load")

        event_list.append(event)

    def load_start_event(self, event_list: List[Union[Callable, Coroutine]]):
        for event in event_list:
            self._load_event(self._start_event_list, event)

    def load_stop_event(self, event_list: List[Union[Callable, Coroutine]]):
        for event in event_list:
            self._load_event(self._stop_event_list, event)

    def load_middleware(self, middleware_list: List[BaseMiddleware]):
        for middleware in middleware_list:
            if middleware not in self._depend_set:
                self._depend_set.add(middleware)
            else:
                raise ImportError(f"{middleware} middleware already load")

            if isinstance(middleware, BaseConnMiddleware):
                middleware.load_sub_middleware(self._conn_handle)
                self._conn_handle = middleware
            elif isinstance(middleware, BaseMiddleware):
                self._middleware_list.append(middleware)
            else:
                raise RuntimeError(f"{middleware} must instance of {BaseMiddleware}")

            if middleware.start_event_list:
                self.load_start_event(middleware.start_event_list)
            if middleware.stop_event_list:
                self.load_stop_event(middleware.stop_event_list)

    def load_processor(self, processor_list: List[BaseProcessor]):
        for processor in processor_list:
            if processor not in self._depend_set:
                self._depend_set.add(processor)
            else:
                raise ImportError(f"{processor} processor already load")
            if isinstance(processor, BaseProcessor):
                self._processor_list.append(processor)
            else:
                raise RuntimeError(f"{processor} must instance of {BaseProcessor}")

            if processor.start_event_list:
                self.load_start_event(processor.start_event_list)
            if processor.stop_event_list:
                self.load_stop_event(processor.stop_event_list)

    @staticmethod
    def register(func: Optional[Callable], name: Optional[str] = None, group: str = "normal"):
        func_manager.register(func, name, group=group)

    @staticmethod
    async def run_callback_list(callback_list: List[Union[Callable, Coroutine]]):
        if not callback_list:
            return
        for callback in callback_list:
            if asyncio.iscoroutine(callback):
                await callback
            else:
                await asyncio.get_event_loop().run_in_executor(None, callback)

    async def create_server(self) -> "Server":
        await self.run_callback_list(self._start_event_list)
        for host in self._host:
            ip, port = host.split(":")
            self._server_list.append(
                await asyncio.start_server(self.conn_handle, ip, port, ssl=self._ssl_context, backlog=self._backlog)
            )
        logging.info(f"server running on {self._host}. use ssl:{bool(self._ssl_context)}")
        return self

    async def await_closed(self):
        for server in self._server_list:
            server.close()
            await server.wait_closed()
            await self.run_callback_list(self._stop_event_list)

    async def conn_handle(self, reader: READER_TYPE, writer: WRITER_TYPE):
        conn: ServerConnection = ServerConnection(reader, writer, self._timeout)
        await self._conn_handle(conn)

    async def _conn_handle(self, conn: ServerConnection):
        response_handle: Response = Response(conn, self._timeout, filter_list=self._processor_list)
        request_handle: Request = Request(
            conn,
            self._run_timeout,
            response_handle,
            self._ping_fail_cnt,
            self._ping_sleep_time,
            filter_list=self._processor_list,
        )
        for middleware in self._middleware_list:
            if isinstance(middleware, BaseMsgMiddleware):
                middleware.load_sub_middleware(request_handle.msg_handle)
                request_handle.msg_handle = middleware

        async def recv_msg_handle(_request_msg: Optional[BASE_REQUEST_TYPE]):
            if _request_msg is None:
                await response_handle(ResponseModel(body=Event(Constant.EVENT_CLOSE_CONN, "request is empty")))
                return
            try:
                request: RequestModel = RequestModel(*_request_msg)
                request.header["_host"] = conn.peer_tuple
            except Exception as closer_e:
                logging.error(f"{conn.peer_tuple} send bad msg:{_request_msg}, error:{closer_e}")
                await response_handle(ResponseModel(body=Event(Constant.EVENT_CLOSE_CONN, "protocol error")))
                await conn.wait_closed()
                return

            try:
                response: Optional[ResponseModel] = await request_handle.dispatch(request)
                await response_handle(response)
            except Exception as closer_e:
                logging.exception(f"raw_request handle error e")
                await response_handle(ResponseModel(body=RpcRunTimeError(str(closer_e))))

        while not conn.is_closed():
            try:
                request_msg: Optional[BASE_REQUEST_TYPE] = await conn.read(self._keep_alive)
                # create future handle msg
                asyncio.ensure_future(recv_msg_handle(request_msg))
            except asyncio.TimeoutError:
                logging.error(f"recv data from {conn.peer_tuple} timeout. close conn")
                await response_handle(ResponseModel(body=Event(Constant.EVENT_CLOSE_CONN, "keep alive timeout")))
                break
            except IOError as e:
                logging.debug(f"close conn:%s info:%s", conn.peer_tuple, e)
                break
            except Exception as e:
                logging.error(f"recv data from {conn.peer_tuple} error:{e}, conn has been closed")
                conn.set_reader_exc(e)
                raise e

        if not conn.is_closed():
            conn.close()
            logging.debug(f"close connection: %s", conn.peer_tuple)
