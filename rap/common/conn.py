import asyncio
import logging
import ssl

from typing import Optional

import msgpack

from rap.common.utlis import Constant, get_event_loop
from rap.common.types import (
    LOOP_TYPE,
    READER_TYPE,
    WRITER_TYPE,
    UNPACKER_TYPE,
)

__all__ = ["Connection", "ServerConnection"]


class BaseConnection:
    def __init__(
            self, unpacker: UNPACKER_TYPE, timeout: int, pack_param: Optional[dict] = None,
            loop: Optional[LOOP_TYPE] = None
    ):
        self._is_closed: bool = True
        self._loop: LOOP_TYPE = loop if loop else get_event_loop()
        self._pack_param: dict = pack_param if pack_param else dict()
        self._reader: Optional[READER_TYPE] = None
        self._timeout: int = timeout
        self._unpacker: UNPACKER_TYPE = unpacker
        self._writer: Optional[WRITER_TYPE] = None

        self.peer: Optional[str] = None
        self.connection_info: Optional[str] = None

    async def write(self, data: tuple, timeout: Optional[int] = None):
        logging.debug(f"sending {data} to {self.peer}")
        self._writer.write(msgpack.packb(data, **self._pack_param))
        timeout = timeout if timeout else self._timeout
        await asyncio.wait_for(self._writer.drain(), timeout)

    async def read(self, timeout: Optional[int] = None) -> tuple:
        try:
            response = next(self._unpacker)
        except StopIteration:
            response = None

        timeout = timeout if timeout else self._timeout
        while True:
            data = await asyncio.wait_for(self._reader.read(Constant.SOCKET_RECV_SIZE), timeout)
            logging.debug(f"recv data {data} from {self.peer}")
            if not data:
                raise ConnectionError(f"Connection to {self.peer} closed")
            self._unpacker.feed(data)
            try:
                response = next(self._unpacker)
                break
            except StopIteration:
                continue
        return response

    def set_reader_exc(self, exc: Exception):
        self._reader.set_exception(exc)

    def close(self):
        self._reader.feed_eof()
        self._writer.close()
        self._is_closed = True

    def is_closed(self) -> bool:
        return self._is_closed and self._writer.is_closing()

    async def wait_closed(self):
        await self._writer.wait_closed()

    async def await_close(self):
        self.close()
        await self.wait_closed()


class Connection(BaseConnection):
    def __init__(
            self,
            unpacker: UNPACKER_TYPE,
            timeout: int,
            pack_param: Optional[dict] = None,
            loop: Optional[LOOP_TYPE] = None,
            ssl_crt_path: Optional[str] = None,
    ):
        super().__init__(unpacker, timeout, pack_param, loop)
        self.connection_info: Optional[str] = None
        self._ssl_crt_path: Optional[str] = ssl_crt_path

    async def connect(self, host: str, port: int):
        self.connection_info: str = f"{host}:{port}"

        ssl_context: Optional[ssl.SSLContext] = None
        if self._ssl_crt_path:
            ssl_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
            ssl_context.check_hostname = False
            ssl_context.load_verify_locations(self._ssl_crt_path)
            logging.info(f"connection enable ssl")

        self._reader, self._writer = await asyncio.open_connection(host, port, loop=self._loop, ssl=ssl_context)
        self.peer = self._writer.get_extra_info("peername")
        self._is_closed = False


class ServerConnection(Connection):
    def __init__(
            self,
            reader: READER_TYPE,
            writer: WRITER_TYPE,
            unpacker: UNPACKER_TYPE,
            timeout: int,
            pack_param: Optional[dict] = None,
            loop: Optional[LOOP_TYPE] = None,
    ):
        super().__init__(unpacker, timeout, pack_param, loop)
        self._reader = reader
        self._writer = writer
        self.peer = self._writer.get_extra_info("peername")
        self._is_closed = False
