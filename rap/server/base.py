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
from rap.middleware import (
    BaseConnMiddleware,
    IpBlockMiddleware
)
from rap.server.requests import Request, ResultModel
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
            secret_list: Optional[List[str]] = None,
            ssl_crt_path: Optional[str] = None,
            ssl_key_path: Optional[str] = None,
    ):
        self._host: str = host
        self._port: int = port
        self._timeout: int = timeout
        self._keep_alive: int = keep_alive
        self._run_timeout: int = run_timeout
        self._ssl_crt_path: Optional[str] = ssl_crt_path
        self._ssl_key_path: Optional[str] = ssl_key_path
        self._conn_middleware: List[BaseConnMiddleware] = [IpBlockMiddleware()]
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
        return await asyncio.start_server(self.conn_handle, self._host, self._port, ssl=ssl_context)

    async def conn_handle(self, reader: READER_TYPE, writer: WRITER_TYPE):
        conn: ServerConnection = ServerConnection(
            reader,
            writer,
            msgpack.Unpacker(use_list=False),
            self._timeout,
            pack_param={'use_bin_type': False},
        )
        logging.debug(f'new connection: {conn.peer}')
        for middleware in self._conn_middleware:
            await middleware.dispatch(conn)

        request_handle = Request(conn, self._run_timeout)

        while not conn.is_closed():
            try:
                request: Optional[BASE_REQUEST_TYPE] = await conn.read(self._keep_alive)
            except asyncio.TimeoutError:
                logging.error(f"recv data from {conn.peer} timeout...")
                await response(conn, self._timeout, event=('close conn', 'read msg timeout'))
                break
            except IOError as e:
                logging.debug(f"close conn:{conn.peer} info:{e}")
                break
            except Exception as e:
                await response(conn, self._timeout, event=('close conn', 'recv error'))
                conn.set_reader_exc(e)
                raise e

            try:
                request_model: ResultModel = await request_handle.dispatch(request)
                await response(
                    conn,
                    self._timeout,
                    crypto=request_model.crypto,
                    response_num=request_model.response_num,
                    msg_id=request_model.msg_id,
                    exception=request_model.exception,
                    result=request_model.result
                )
            except Exception as e:
                logging.error(traceback.format_exc())
                await response(conn, self._timeout, exception=e)

        if not conn.is_closed():
            conn.close()
            logging.debug(f"close connection: {conn.peer}")
