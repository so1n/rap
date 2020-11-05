"""

"""
import asyncio
import logging

from collections import deque
from typing import Coroutine, Deque, List, Set, Optional

from rap.conn.connection import Connection
from rap.common.types import (
    UNPACKER_TYPE,
)


class CloseEvent:
    def __init__(self, on_close):
        self._close_init = asyncio.Event()
        self._close_done = asyncio.Event()
        self._on_close = on_close

    async def wait(self):
        await self._close_init.wait()
        await self._close_done.wait()

    def is_set(self):
        return self._close_done.is_set() or self._close_init.is_set()

    def set(self):
        if self._close_init.is_set():
            return

        task = asyncio.ensure_future(self._on_close())
        task.add_done_callback(self._cleanup)
        self._close_init.set()

    def _cleanup(self, *args):
        self._on_close = None
        self._close_done.set()


class Pool(object):
    """Simple connection pool, design like aioredis.pool"""

    def __init__(
        self,
        host: str,
        port: int,
        unpacker: UNPACKER_TYPE,
        timeout: int,
        min_size: int = 1,
        max_size: int = 10,
        ssl_crt_path: Optional[str] = None,
    ):
        self._host: str = host
        self._port: int = port
        self._unpacker: UNPACKER_TYPE = unpacker
        self._timeout: int = timeout
        self._ssl_srt_path: Optional[str] = ssl_crt_path

        self._min_size = min_size
        self._max_size = max_size

        self._is_closed: bool = True
        self._cond = asyncio.Condition()
        self._close_state = CloseEvent(self._do_close)
        self._pool: Deque[Connection] = deque(maxlen=max_size)
        self._used_set: Set[Connection] = set()
        self.connection_info: str = f"{host}:{port}"

    @property
    def free_size(self):
        """Current number of free connections."""
        return len(self._pool)

    @property
    def size(self):
        return self.free_size + self.use_size

    @property
    def use_size(self):
        return len(self._used_set)

    def _drop_closed(self):
        for i in range(self.free_size):
            conn = self._pool[0]
            if conn.is_closed():
                self._pool.popleft()
            else:
                self._pool.rotate(-1)

    async def _do_close(self):
        async with self._cond:
            waiter_list: List[Coroutine] = []
            while self._pool:
                waiter_list.append(self._pool.popleft().await_close())
            for conn in self._used_set:
                waiter_list.append(conn.await_close())
            await asyncio.gather(*waiter_list)
            logging.debug("Closed %d connection(s)", len(waiter_list))

    def is_closed(self):
        return self._close_state.is_set()

    async def wait_closed(self):
        await self._close_state.wait()

    async def await_close(self):
        self.close()
        await self.wait_closed()

    def close(self):
        if not self._close_state.is_set():
            self._close_state.set()

    async def connect(self):
        self._drop_closed()
        while self.size < self._max_size:
            try:
                conn = Connection(self._unpacker, self._timeout, ssl_crt_path=self._ssl_srt_path)
                await conn.connect(self._host, self._port)
                self._pool.append(conn)
            finally:
                self._drop_closed()
                await asyncio.sleep(0.01)

    async def acquire(self):
        async with self._cond:
            while True:
                await self.connect()
                if self.free_size:
                    conn = self._pool.popleft()
                    self._used_set.add(conn)
                    return conn
                else:
                    await self._cond.wait()

    def release(self, conn: Connection):
        if conn not in self._used_set:
            raise RuntimeError("connection not in user pool")
        self._used_set.remove(conn)
        if conn.is_closed():
            asyncio.ensure_future(self._wakeup(conn))
        elif self.free_size < self._max_size:
            self._pool.append(conn)
            asyncio.ensure_future(self._wakeup())
        else:
            asyncio.ensure_future(self._wakeup(conn))

    async def _wakeup(self, conn: Optional[Connection] = None):
        async with self._cond:
            self._cond.notify()
        if conn is not None:
            conn.close()
