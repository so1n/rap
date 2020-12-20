import asyncio
import logging
import ssl

import msgpack

from typing import Callable, Coroutine, List, Optional, Union

from rap.common.conn import ServerConnection
from rap.common.exceptions import RpcRunTimeError
from rap.common.types import READER_TYPE, WRITER_TYPE, BASE_REQUEST_TYPE
from rap.common.utlis import Constant, Event
from rap.manager.func_manager import func_manager
from rap.server.middleware.base import (
    BaseConnMiddleware,
    BaseMsgMiddleware,
    BaseRequestMiddleware,
    BaseResponseMiddleware,
)
from rap.common.middleware import BaseMiddleware
from rap.server.requests import Request, RequestModel
from rap.server.response import Response, ResponseModel


__all__ = ["Server"]


class Server(object):
    def __init__(
        self,
        host: str = "localhost",
        port: int = 9000,
        timeout: int = 9,
        keep_alive: int = 1200,
        run_timeout: int = 9,
        backlog: int = 1024,
        ssl_crt_path: Optional[str] = None,
        ssl_key_path: Optional[str] = None,
        connect_call_back: List[Union[Callable, Coroutine]] = None,
        close_call_back: List[Union[Callable, Coroutine]] = None,
    ):
        self._host: str = host
        self._port: int = port
        self._timeout: int = timeout
        self._run_timeout: int = run_timeout
        self._keep_alive: int = keep_alive
        self._backlog: int = backlog

        self._connect_callback: List[Union[Callable, Coroutine]] = connect_call_back
        self._close_callback: List[Union[Callable, Coroutine]] = close_call_back

        self._ssl_context: Optional[ssl.SSLContext] = None
        if ssl_crt_path and ssl_key_path:
            self._ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
            self._ssl_context.check_hostname = False
            self._ssl_context.load_cert_chain(ssl_crt_path, ssl_key_path)

        self._server: Optional[asyncio.AbstractServer] = None
        self._middleware_list: List[BaseMiddleware] = []

    def load_middleware(self, middleware_list: List[BaseMiddleware]):
        for middleware in middleware_list:
            if isinstance(middleware, BaseConnMiddleware):
                middleware.load_sub_middleware(self._conn_handle)
                self._conn_handle = middleware
            elif isinstance(middleware, BaseMiddleware):
                self._middleware_list.append(middleware)
            else:
                raise RuntimeError(f'{middleware} must instance of {BaseMiddleware}')

    @staticmethod
    def register(func: Optional[Callable], name: Optional[str] = None, group: str = "normal"):
        func_manager.register(func, name, group=group)

    @staticmethod
    async def run_callback(callback_list: List[Union[Callable, Coroutine]]):
        if not callback_list:
            return
        for callback in callback_list:
            if asyncio.iscoroutine(callback):
                await callback
            else:
                await asyncio.get_event_loop().run_in_executor(None, callback)

    async def create_server(self) -> "Server":
        await self.run_callback(self._connect_callback)
        self._server = await asyncio.start_server(
            self.conn_handle, self._host, self._port, ssl=self._ssl_context, backlog=self._backlog
        )
        logging.info(f"server running on {self._host}:{self._port}. use ssl:{bool(self._ssl_context)}")
        return self

    async def wait_closed(self):
        if self._server:
            self._server.close()
            await self._server.wait_closed()
        await self.run_callback(self._close_callback)

    async def conn_handle(self, reader: READER_TYPE, writer: WRITER_TYPE):
        conn: ServerConnection = ServerConnection(
            reader,
            writer,
            msgpack.Unpacker(raw=False, use_list=False),
            self._timeout,
        )
        await self._conn_handle(conn)

    async def _conn_handle(self, conn: ServerConnection):
        response_handle: Response = Response(conn, self._timeout)
        request_handle: Request = Request(conn, self._run_timeout)
        for middleware in self._middleware_list:
            if isinstance(middleware, BaseRequestMiddleware):
                middleware.load_sub_middleware(request_handle.real_dispatch)
                request_handle.real_dispatch = middleware
            elif isinstance(middleware, BaseMsgMiddleware):
                middleware.load_sub_middleware(request_handle.msg_handle)
                request_handle.msg_handle = middleware
            elif isinstance(middleware, BaseResponseMiddleware):
                middleware.load_sub_middleware(response_handle.response_handle)
                request_handle.response_handle = middleware

        async def recv_msg_handle(_request_msg: Optional[BASE_REQUEST_TYPE]):
            if _request_msg is None:
                await response_handle(ResponseModel(body=Event(Constant.EVENT_CLOSE_CONN, "request is empty")))
                return
            try:
                request_num, msg_id, header, body = _request_msg
                request: RequestModel = RequestModel(request_num, msg_id, header, body)
                request.header["_host"] = conn.peer
            except Exception as closer_e:
                logging.error(f"{conn.peer} send bad msg:{_request_msg}, error:{closer_e}")
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
                asyncio.ensure_future(recv_msg_handle(request_msg))
            except asyncio.TimeoutError:
                logging.error(f"recv data from {conn.peer} timeout. close conn")
                await response_handle(ResponseModel(body=Event(Constant.EVENT_CLOSE_CONN, "keep alive timeout")))
                break
            except IOError as e:
                logging.debug(f"close conn:%s info:%s", conn.peer, e)
                break
            except Exception as e:
                logging.error(f"recv data from {conn.peer} error:{e}, conn has been closed")
                conn.set_reader_exc(e)
                raise e

        if not conn.is_closed():
            conn.close()
            logging.debug(f"close connection: %s", conn.peer)
