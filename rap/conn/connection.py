import asyncio
import logging

from typing import Optional

import msgpack

from rap.utlis import (
    Constant,
    get_event_loop
)
from rap.types import (
    LOOP_TYPE,
    READER_TYPE,
    WRITER_TYPE,
    UNPACKER_TYPE,
)

__all__ = ['Connection', 'ServerConnection']


class BaseConnection:
    def __init__(
            self,
            unpacker: UNPACKER_TYPE,
            timeout: int,
            pack_param: Optional[dict] = None,
            loop: Optional[LOOP_TYPE] = None
    ):
        self._reader: Optional[READER_TYPE] = None
        self._writer: Optional[WRITER_TYPE] = None
        self._pack_param: dict = pack_param if pack_param else dict()
        self.unpacker: UNPACKER_TYPE = unpacker
        self._is_closed: bool = True
        self.timeout: int = timeout
        self.peer: Optional[str] = None
        self._loop: LOOP_TYPE = loop if loop else get_event_loop()

    async def write(self, data: tuple, timeout: Optional[int] = None):
        logging.debug(f'sending {data} to {self.peer}')
        self._writer.write(msgpack.packb(data, **self._pack_param))
        timeout = timeout if timeout else self.timeout
        await asyncio.wait_for(self._writer.drain(), timeout)

    async def read(self, timeout=None) -> tuple:
        timeout = timeout if timeout else self.timeout
        response = None
        while True:
            data = await asyncio.wait_for(self._reader.read(Constant.SOCKET_RECV_SIZE), timeout)
            logging.debug(f'recv data {data} from {self.peer}')
            if not data:
                raise ConnectionError(f'Connection to {self.peer} closed')
            self.unpacker.feed(data)
            try:
                response = next(self.unpacker)
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
    async def connect(self, host: str, port: int):
        self._reader, self._writer = await asyncio.open_connection(host, port, loop=self._loop)
        self.peer = self._writer.get_extra_info('peername')
        self._is_closed = False


class ServerConnection(Connection):

    def __init__(
            self,
            reader: READER_TYPE,
            writer: WRITER_TYPE,
            unpacker: UNPACKER_TYPE,
            timeout: int,
            pack_param: Optional[dict] = None,
            loop: Optional[LOOP_TYPE] = None
    ):
        super().__init__(unpacker, timeout, pack_param, loop)
        self._reader = reader
        self._writer = writer
        self.peer = self._writer.get_extra_info('peername')
        self._is_closed = False
