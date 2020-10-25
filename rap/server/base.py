import asyncio
import logging
import traceback
import ssl

import msgpack

from typing import Callable, List, Optional

from rap.conn.connection import ServerConnection
from rap.manager.aes_manager import aes_manager
from rap.manager.client_manager import client_manager
from rap.manager.func_manager import func_manager
from rap.middleware.base_middleware import (
    BaseConnMiddleware,
    BaseMsgMiddleware,
    BaseRequestMiddleware
)
from rap.server.requests import Request, RequestModel, ResultModel
from rap.server.response import response
from rap.common.types import (
    READER_TYPE,
    WRITER_TYPE,
    BASE_REQUEST_TYPE
)

__all__ = ['Server']


class Server(object):

    def __init__(
            self,
            host: str = 'localhost',
            port: int = 9000,
            timeout: int = 9,
            keep_alive: int = 1200,
            run_timeout: int = 9,
            backlog: int = 1024,
            conn_middleware_list: Optional[List[BaseConnMiddleware]] = None,
            msg_middleware_list: Optional[List[BaseMsgMiddleware]] = None,
            request_middleware_list: Optional[List[BaseRequestMiddleware]] = None,
            secret_list: Optional[List[str]] = None,
            ssl_crt_path: Optional[str] = None,
            ssl_key_path: Optional[str] = None,
    ):
        self._host: str = host
        self._port: int = port
        self._timeout: int = timeout
        self._keep_alive: int = keep_alive
        self._backlog: int = backlog
        self._ssl_crt_path: Optional[str] = ssl_crt_path
        self._ssl_key_path: Optional[str] = ssl_key_path
        self._request_handle = Request(run_timeout)

        _conn_middleware = self._conn_handle
        for conn_middleware in reversed(conn_middleware_list):
            conn_middleware.load_sub_middleware(_conn_middleware)
            _conn_middleware = conn_middleware
        self._conn_middleware = _conn_middleware

        _request_middleware = self._request_handle.dispatch
        for request_middleware in reversed(request_middleware_list):
            request_middleware.load_sub_middleware(_request_middleware)
            _request_middleware = request_middleware
        self._request_middleware = _request_middleware

        if secret_list is not None:
            for secret in secret_list:
                aes_manager.add_aes(secret)

    @staticmethod
    def register(func: Optional[Callable], name: Optional[str] = None):
        func_manager.register(func, name)

    async def create_server(self) -> asyncio.AbstractServer:

        ssl_context: Optional[ssl.SSLContext] = None
        if self._ssl_crt_path and self._ssl_key_path:
            ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
            ssl_context.check_hostname = False
            ssl_context.load_cert_chain(self._ssl_crt_path, self._ssl_key_path)
            logging.info(f"server enable ssl")

        logging.info(f'server running on {self._host}:{self._port}')
        asyncio.ensure_future(client_manager.introspection())
        return await asyncio.start_server(
            self.conn_handle, self._host, self._port, ssl=ssl_context, backlog=self._backlog
        )

    async def conn_handle(self, reader: READER_TYPE, writer: WRITER_TYPE):
        conn: ServerConnection = ServerConnection(
            reader,
            writer,
            msgpack.Unpacker(use_list=False),
            self._timeout,
            pack_param={'use_bin_type': False},
        )
        await self._conn_middleware.dispatch(conn)

    async def _conn_handle(self, conn: ServerConnection):
        logging.debug(f'new connection: {conn.peer}')
        while not conn.is_closed():
            try:
                request: Optional[BASE_REQUEST_TYPE] = await conn.read(self._keep_alive)
            except asyncio.TimeoutError:
                logging.error(f"recv data from {conn.peer} timeout...")
                await response(conn, self._timeout, event=('close conn', 'read request timeout'))
                break
            except IOError as e:
                logging.debug(f"close conn:{conn.peer} info:{e}")
                break
            except Exception as e:
                await response(conn, self._timeout, event=('close conn', 'recv error'))
                conn.set_reader_exc(e)
                raise e
            if request is None:
                await response(conn, self._timeout, event=('close conn', 'request is empty'))
            try:
                request_num, msg_id, header, body = request
                request_model: RequestModel = RequestModel(request_num, msg_id, header, body, conn)
            except Exception:
                await response(conn, self._timeout, event=('close conn', 'protocol error'))
                break

            request_model.header['_host'] = conn.peer
            try:
                result_model: ResultModel = await self._request_middleware(request_model)
                await response(
                    conn,
                    self._timeout,
                    crypto=result_model.crypto,
                    response_num=result_model.response_num,
                    msg_id=result_model.msg_id,
                    exception=result_model.exception,
                    result=result_model.result
                )
            except Exception as e:
                logging.error(traceback.format_exc())
                await response(conn, self._timeout, exception=e)

        if not conn.is_closed():
            conn.close()
            logging.debug(f"close connection: {conn.peer}")

