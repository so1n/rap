import asyncio
import logging
import random
import ssl
import time
from typing import Any, Optional, Tuple

import msgpack

from rap.common.state import State
from rap.common.types import READER_TYPE, UNPACKER_TYPE, WRITER_TYPE
from rap.common.utils import constant

__all__ = ["Connection", "ServerConnection", "CloseConnException"]
logger: logging.Logger = logging.getLogger(__name__)


class CloseConnException(Exception):
    pass


class BaseConnection:
    """rap transmission function, including serialization and deserialization of transmitted data"""

    def __init__(self, pack_param: Optional[dict] = None, unpack_param: Optional[dict] = None):
        self._is_closed: bool = True
        self._pack_param: dict = pack_param or {}
        self._unpack_param: dict = unpack_param or {}
        if "raw" not in self._unpack_param:
            self._unpack_param["raw"] = False
        if "use_list" not in self._unpack_param:
            self._unpack_param["use_list"] = False
        self._unpacker: UNPACKER_TYPE = msgpack.Unpacker(**self._unpack_param)
        self._reader: Optional[READER_TYPE] = None
        self._writer: Optional[WRITER_TYPE] = None
        self._max_msg_id: int = 65535
        self._msg_id: int = random.randrange(self._max_msg_id)
        self.conn_id: str = ""
        self.state: State = State()

        self.conn_future: asyncio.Future = asyncio.Future()
        self.peer_tuple: Tuple[str, int] = ("", -1)
        self.sock_tuple: Tuple[str, int] = ("", -1)

    async def sleep_and_listen(self, delay: float) -> None:
        """Monitor the status of transport while sleeping.
        When the listen of transport is cancelled or terminated, it will automatically exit from sleep
        """
        try:
            await asyncio.wait_for(asyncio.shield(self.conn_future), timeout=delay)
        except asyncio.TimeoutError:
            pass

    async def write(self, data: tuple) -> None:
        if not self._writer or self._is_closed:
            raise ConnectionError("connection has not been created")
        logger.debug("write %s to %s", data, self.peer_tuple)
        self._writer.write(msgpack.packb(data, **self._pack_param))
        await self._writer.drain()

    async def read(self) -> Any:
        if not self._reader or self._is_closed:
            raise ConnectionError("connection has not been created")
        try:
            try:
                data: Any = next(self._unpacker)
                logger.debug("read %s from %s", data, self.peer_tuple)
                return data
            except StopIteration:
                pass

            while True:
                data = await self._reader.read(constant.SOCKET_RECV_SIZE)
                if not data:
                    raise CloseConnException(f"Connection to {self.peer_tuple} closed")
                self._unpacker.feed(data)
                try:
                    data = next(self._unpacker)
                    logger.debug("read %s from %s", data, self.peer_tuple)
                    return data
                except StopIteration:
                    continue
        except Exception as e:
            self.set_reader_exc(e)
            raise e

    def set_reader_exc(self, exc: Exception) -> None:
        if not isinstance(exc, Exception) or self.is_closed():
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
    """rap client connection"""

    def __init__(
        self,
        host: str,
        port: int,
        pack_param: Optional[dict] = None,
        unpack_param: Optional[dict] = None,
        ssl_crt_path: Optional[str] = None,
    ):
        super().__init__(pack_param, unpack_param)
        self._host: str = host
        self._port: int = port
        self._ssl_crt_path: Optional[str] = ssl_crt_path
        self.connection_info: str = f"{host}:{port}"

    @property
    def host(self) -> str:
        return self._host

    @property
    def port(self) -> int:
        return self._port

    async def connect(self) -> None:
        ssl_context: Optional[ssl.SSLContext] = None
        if self._ssl_crt_path:
            ssl_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
            ssl_context.check_hostname = False
            ssl_context.load_verify_locations(self._ssl_crt_path)
            logger.info("connection enable ssl")

        self._reader, self._writer = await asyncio.open_connection(self._host, self._port, ssl=ssl_context)
        self.sock_tuple = self._writer.get_extra_info("sockname")
        self.peer_tuple = self._writer.get_extra_info("peername")
        self.conn_future: asyncio.Future = asyncio.Future()
        self._is_closed = False
        logger.debug("Connection to %s...", self.connection_info)


class ServerConnection(BaseConnection):
    """rap server connection"""

    def __init__(
        self,
        reader: READER_TYPE,
        writer: WRITER_TYPE,
        pack_param: Optional[dict] = None,
        unpack_param: Optional[dict] = None,
    ):
        super().__init__(pack_param, unpack_param)
        self._reader = reader
        self._writer = writer
        self.peer_tuple = self._writer.get_extra_info("peername")
        self.sock_tuple: Tuple[str, int] = self._writer.get_extra_info("sockname")
        self.conn_future = asyncio.Future()
        self._is_closed = False
        self.keepalive_timestamp = int(time.time())
