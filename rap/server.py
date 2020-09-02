import asyncio
import logging

import msgpack

from typing import Callable, List, Optional

from rap.conn.connection import ServerConnection
from rap.exceptions import ServerError
from rap.manager.func_manager import func_manager
from rap.middleware import (
    BaseConnMiddleware,
    IpLimitMiddleware
)
from rap.requests import Request
from rap.response import response
from rap.types import (
    READER_TYPE,
    WRITER_TYPE,
    REQUEST_TYPE
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
            secret: Optional[str] = None
    ):
        self._host: str = host
        self._port: int = port
        self._timeout: int = timeout
        self._keep_alive: int = keep_alive
        self._run_timeout: int = run_timeout
        self._secret: Optional[str] = secret
        self._conn_middleware: List[BaseConnMiddleware] = [IpLimitMiddleware()]

    @staticmethod
    def register(func: Optional[Callable], name: Optional[str] = None):
        func_manager.register(func, name)

    async def create_server(self) -> asyncio.AbstractServer:
        logging.info(f'server running on {self._host}:{self._port}')
        return await asyncio.start_server(self.conn_handle, self._host, self._port)

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

        request_handle = Request(
            conn,
            self._timeout,
            self._run_timeout,
            secret=self._secret
        )

        while not conn.is_closed():
            try:
                request: Optional[REQUEST_TYPE] = await conn.read(self._keep_alive)
            except asyncio.TimeoutError:
                logging.error(f"recv data from {conn.peer} timeout...")
                break
            except IOError as e:
                logging.debug(f"close conn:{conn.peer} info:{e}")
                break
            except Exception as e:
                await response(conn, self._timeout, exception=ServerError(), is_auth=False)
                conn.set_reader_exc(e)
                raise e
            await request_handle.msg_handle(request)

        if not conn.is_closed():
            conn.close()
            logging.debug(f"close connection: {conn.peer}")
