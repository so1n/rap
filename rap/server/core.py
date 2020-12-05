import asyncio
import logging
import ssl

import msgpack

from typing import Callable, List, Optional

from rap.common.conn import ServerConnection
from rap.common.exceptions import RpcRunTimeError
from rap.common.types import READER_TYPE, WRITER_TYPE, BASE_REQUEST_TYPE
from rap.manager.client_manager import client_manager
from rap.manager.func_manager import func_manager
from rap.server.middleware.base import (
    BaseConnMiddleware,
    BaseMsgMiddleware,
    BaseRawRequestMiddleware,
    BaseRequestDispatchMiddleware,
    BaseResponseMiddleware
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
    ):
        self._host: str = host
        self._port: int = port
        self._timeout: int = timeout
        self._keep_alive: int = keep_alive
        self._backlog: int = backlog
        self._request: Request = Request(run_timeout)
        self._response: Response = Response(timeout)

        self._ssl_context: Optional[ssl.SSLContext] = None
        if ssl_crt_path and ssl_key_path:
            self._ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
            self._ssl_context.check_hostname = False
            self._ssl_context.load_cert_chain(ssl_crt_path, ssl_key_path)

    def load_middleware(self, middleware_list: List[BaseMiddleware]):
        for middleware in reversed(middleware_list):
            if isinstance(middleware, BaseConnMiddleware):
                middleware.load_sub_middleware(self._conn_handle)
                self._conn_handle = middleware
            elif isinstance(middleware, BaseRawRequestMiddleware):
                middleware.load_sub_middleware(self._request.before_dispatch)
                self._request.before_dispatch = middleware
            elif isinstance(middleware, BaseRequestDispatchMiddleware):
                middleware.load_sub_middleware(self._request.dispatch)
                self._request.dispatch = middleware
            elif isinstance(middleware, BaseMsgMiddleware):
                middleware.load_sub_middleware(self._request.msg_handle)
                self._request.msg_handle = middleware
            elif isinstance(middleware, BaseResponseMiddleware):
                middleware.load_sub_middleware(self._response.response_handle)
                self._response.response_handle = middleware

    @staticmethod
    def register(func: Optional[Callable], name: Optional[str] = None):
        func_manager.register(func, name)

    async def create_server(self) -> asyncio.AbstractServer:
        logging.info(f"server running on {self._host}:{self._port}. use ssl:{bool(self._ssl_context)}")
        asyncio.ensure_future(client_manager.introspection())
        return await asyncio.start_server(
            self.conn_handle, self._host, self._port, ssl=self._ssl_context, backlog=self._backlog
        )

    async def conn_handle(self, reader: READER_TYPE, writer: WRITER_TYPE):
        conn: ServerConnection = ServerConnection(
            reader,
            writer,
            msgpack.Unpacker(raw=False, use_list=False),
            self._timeout,
        )
        await self._conn_handle(conn)

    async def _conn_handle(self, conn: ServerConnection):
        while not conn.is_closed():
            try:
                request: Optional[BASE_REQUEST_TYPE] = await conn.read(self._keep_alive)
            except asyncio.TimeoutError:
                logging.error(f"recv data from {conn.peer} timeout. close conn")
                await self._response(conn, ResponseModel(event=("close conn", "keep alive timeout")))
                break
            except IOError as e:
                logging.debug(f"close conn:%s info:%s", conn.peer, e)
                break
            except Exception as e:
                logging.error(f"recv data from {conn.peer} error:{e}, conn has been closed")
                conn.set_reader_exc(e)
                raise e
            if request is None:
                await self._response(conn, ResponseModel(event=("close conn", "raw_request is empty")))
                continue
            try:
                request_num, msg_id, header, body = request
                request_model: RequestModel = RequestModel(request_num, msg_id, header, body, conn)
                request_model.header["_host"] = conn.peer
                asyncio.ensure_future(self.request_handle(conn, request_model))
            except Exception as e:
                logging.error(f"{conn.peer} send bad msg:{request}, error:{e}")
                await self._response(conn, ResponseModel(event=("close conn", "protocol error")))
                break

        if not conn.is_closed():
            conn.close()
            logging.debug(f"close connection: %s", conn.peer)

    async def request_handle(self, conn: ServerConnection, request_model: RequestModel):
        try:
            resp_model: ResponseModel = await self._request.before_dispatch(request_model)
            await self._response(conn, resp_model)
        except Exception as e:
            logging.exception(f"raw_request handle error e")
            await self._response(conn, ResponseModel(exception=RpcRunTimeError(str(e))))