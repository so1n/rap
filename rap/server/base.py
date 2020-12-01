import asyncio
import logging
import ssl

import msgpack

from typing import Callable, Dict, List, Optional, Union

from rap.common.conn import ServerConnection
from rap.common.exceptions import RpcRunTimeError
from rap.common.types import READER_TYPE, WRITER_TYPE, BASE_REQUEST_TYPE
from rap.manager.aes_manager import aes_manager
from rap.manager.client_manager import client_manager
from rap.manager.func_manager import func_manager
from rap.middleware.base_middleware import BaseConnMiddleware, BaseMsgMiddleware, BaseRequestMiddleware
from rap.server.requests import Request, RequestModel
from rap.server.response import response, ResponseModel


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
        conn_middleware_list: Optional[List[BaseConnMiddleware]] = None,
        msg_middleware_list: Optional[List[BaseMsgMiddleware]] = None,
        request_middleware_list: Optional[List[BaseRequestMiddleware]] = None,
        secret_dict: Optional[Dict[str, str]] = None,
        ssl_crt_path: Optional[str] = None,
        ssl_key_path: Optional[str] = None,
    ):
        self._host: str = host
        self._port: int = port
        self._timeout: int = timeout
        self._keep_alive: int = keep_alive
        self._backlog: int = backlog
        self._request_handle: Request = Request(run_timeout)

        self._ssl_context: Optional[ssl.SSLContext] = None
        if ssl_crt_path and ssl_key_path:
            self._ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
            self._ssl_context.check_hostname = False
            self._ssl_context.load_cert_chain(ssl_crt_path, ssl_key_path)

        if secret_dict is not None:
            aes_manager.load_aes_key_dict(secret_dict)

        # replace func -> *_middleware
        if conn_middleware_list is not None:
            _conn_middleware: Union[Callable, BaseConnMiddleware] = self._conn_handle
            for conn_middleware in reversed(conn_middleware_list):
                conn_middleware.load_sub_middleware(_conn_middleware)
                _conn_middleware = conn_middleware
            self._conn_handle = _conn_middleware

        if request_middleware_list is not None:
            _request_middleware: Union[Callable, BaseRequestMiddleware] = self._request_handle.dispatch
            for request_middleware in reversed(request_middleware_list):
                request_middleware.load_sub_middleware(_request_middleware)
                _request_middleware = request_middleware
            self._request_handle.dispatch = _request_middleware

        if msg_middleware_list is not None:
            _msg_middleware: Union[Callable, BaseMsgMiddleware] = self._request_handle.msg_handle
            for msg_middleware in reversed(msg_middleware_list):
                msg_middleware.load_sub_middleware(_msg_middleware)
                _msg_middleware = msg_middleware
            self._request_handle.msg_handle = _msg_middleware

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
                await response(conn, ResponseModel(event=("close conn", "keep alive timeout")))
                break
            except IOError as e:
                logging.debug(f"close conn:%s info:%s", conn.peer, e)
                break
            except Exception as e:
                logging.error(f"recv data from {conn.peer} error:{e}, conn has been closed")
                conn.set_reader_exc(e)
                raise e
            if request is None:
                await response(conn, ResponseModel(event=("close conn", "request is empty")))
                continue
            try:
                request_num, msg_id, header, body = request
                request_model: RequestModel = RequestModel(request_num, msg_id, header, body, conn)
                request_model.header["_host"] = conn.peer
                asyncio.ensure_future(self.request_handle(conn, request_model))
            except Exception as e:
                logging.error(f"{conn.peer} send bad msg:{request}, error:{e}")
                await response(conn, ResponseModel(event=("close conn", "protocol error")))
                break

        if not conn.is_closed():
            conn.close()
            logging.debug(f"close connection: %s", conn.peer)

    async def request_handle(self, conn: ServerConnection, request_model: RequestModel):
        try:
            resp_model: ResponseModel = await self._request_handle.dispatch(request_model)
            await response(conn, resp_model)
        except Exception as e:
            logging.exception(f"request handle error e")
            await response(conn, ResponseModel(exception=RpcRunTimeError(str(e))))
