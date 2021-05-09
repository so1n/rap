import asyncio
import logging
import ssl
from typing import Any, Optional, Tuple

import msgpack  # type: ignore

from rap.common.types import BASE_RESPONSE_TYPE, READER_TYPE, UNPACKER_TYPE, WRITER_TYPE
from rap.common.utils import Constant

__all__ = ["Connection", "ServerConnection"]


class BaseConnection:
    def __init__(self, timeout: int, pack_param: Optional[dict] = None):
        self._is_closed: bool = True
        self._timeout: int = timeout
        self._pack_param: dict = pack_param if pack_param else dict()
        self._unpacker: UNPACKER_TYPE = msgpack.Unpacker(raw=False, use_list=False)
        self._reader: Optional[READER_TYPE] = None
        self._writer: Optional[WRITER_TYPE] = None

        self.conn_future: asyncio.Future = asyncio.Future()
        self.peer_tuple: Tuple[str, int] = ("", -1)
        self.sock_tuple: Tuple[str, int] = ("", -1)

    async def write(self, data: tuple, timeout: Optional[int] = None) -> None:
        if not self._writer or self._is_closed:
            raise ConnectionError("connection has not been created")
        logging.debug("write %s to %s", data, self.peer_tuple)
        self._writer.write(msgpack.packb(data, **self._pack_param))
        timeout = timeout if timeout else self._timeout
        await asyncio.wait_for(self._writer.drain(), timeout)

    async def read(self, timeout: Optional[int] = None) -> Optional[BASE_RESPONSE_TYPE]:
        if not self._reader or self._is_closed:
            raise ConnectionError("connection has not been created")
        try:
            try:
                data: Any = next(self._unpacker)
                logging.debug(f"read %s from %s", data, self.peer_tuple)
                return data
            except StopIteration:
                pass

            timeout = timeout if timeout else self._timeout
            while True:
                data = await asyncio.wait_for(self._reader.read(Constant.SOCKET_RECV_SIZE), timeout)
                if not data:
                    raise ConnectionError(f"Connection to {self.peer_tuple} closed")
                self._unpacker.feed(data)
                try:
                    data = next(self._unpacker)
                    logging.debug(f"read %s from %s", data, self.peer_tuple)
                    return data
                except StopIteration:
                    continue
        except Exception as e:
            self.set_reader_exc(e)
            raise e

    def set_reader_exc(self, exc: Exception) -> None:
        if not isinstance(exc, Exception):
            return

        if self.conn_future and not self.conn_future.done():
            self.conn_future.set_exception(exc)
        if self._reader:
            self._reader.set_exception(exc)

    def close(self) -> None:
        if self._reader:
            self._reader.feed_eof()
        if self._writer:
            self._writer.close()

        self._is_closed = True

        if self.conn_future and not self.conn_future.cancelled():
            self.conn_future.cancel()

    def is_closed(self) -> bool:
        if self._writer:
            return self._is_closed and self._writer.is_closing()
        else:
            return self._is_closed

    async def wait_closed(self) -> None:
        if self._writer:
            await self._writer.wait_closed()

    async def await_close(self) -> None:
        self.close()
        await self.wait_closed()


class Connection(BaseConnection):
    def __init__(
        self,
        timeout: int,
        pack_param: Optional[dict] = None,
        ssl_crt_path: Optional[str] = None,
    ):
        super().__init__(timeout, pack_param)
        self.connection_info: Optional[str] = None
        self._ssl_crt_path: Optional[str] = ssl_crt_path

    async def connect(self, host: str, port: int) -> None:
        self.connection_info = f"{host}:{port}"

        ssl_context: Optional[ssl.SSLContext] = None
        if self._ssl_crt_path:
            ssl_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
            ssl_context.check_hostname = False
            ssl_context.load_verify_locations(self._ssl_crt_path)
            logging.info(f"connection enable ssl")

        self._reader, self._writer = await asyncio.open_connection(host, port, ssl=ssl_context)
        self.sock_tuple = self._writer.get_extra_info("sockname")
        self.peer_tuple = self._writer.get_extra_info("peername")
        self.conn_future: asyncio.Future = asyncio.Future()
        self._is_closed = False


class ServerConnection(BaseConnection):
    def __init__(
        self,
        reader: READER_TYPE,
        writer: WRITER_TYPE,
        timeout: int,
        pack_param: Optional[dict] = None,
    ):
        super().__init__(timeout, pack_param)
        self._reader = reader
        self._writer = writer
        self.peer_tuple = self._writer.get_extra_info("peername")
        self.sock_tuple: Tuple[str, int] = self._writer.get_extra_info("sockname")
        self.conn_future = asyncio.Future()
        self._is_closed = False
