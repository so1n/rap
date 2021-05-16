import logging
import time
from typing import Callable, Dict, Optional, Union

from aredis import StrictRedis, StrictRedisCluster  # type: ignore

from rap.common.conn import ServerConnection
from rap.common.utils import Constant, Event
from rap.server.middleware.base import BaseConnMiddleware
from rap.server.model import Response
from rap.server.sender import Sender


class ConnLimitMiddleware(BaseConnMiddleware):
    """
    feat: limit server max conn num
    """

    def __init__(self, max_conn: int = 1024, block_time: int = 30):
        self._max_conn: int = max_conn
        self._conn_count: int = 0
        self._block_time: int = block_time
        self._release_timestamp: int = int(time.time())

    def start_event_handle(self) -> None:
        self.register(self._get_conn_limit_info)
        self.register(self._modify_max_conn)
        self.register(self._modify_release_timestamp)

    def _get_conn_limit_info(self) -> Dict[str, int]:
        return {
            "release_timestamp": self._release_timestamp,
            "conn_count": self._conn_count,
            "max_conn": self._max_conn,
        }

    def _modify_release_timestamp(self, timestamp: int) -> None:
        self._release_timestamp = timestamp

    def _modify_max_conn(self, max_conn: int) -> None:
        self._max_conn = max_conn

    async def dispatch(self, conn: ServerConnection) -> None:
        now_timestamp: int = int(time.time())
        self._conn_count += 1
        try:
            if self._release_timestamp > now_timestamp or self._conn_count > self._max_conn:
                self._release_timestamp = now_timestamp + self._block_time
                await Sender(conn)(
                    Response.from_event(
                        Event(Constant.EVENT_CLOSE_CONN, "Currently exceeding the maximum number of connections limit")
                    )
                )
                await conn.await_close()
                return
            else:
                await self.call_next(conn)
        finally:
            self._conn_count -= 1


class IpMaxConnMiddleware(BaseConnMiddleware):
    """
    feat: Limit the number of connections of a specified IP within a unit time
    """

    def __init__(self, redis: Union[StrictRedis, StrictRedisCluster], ip_max_conn: int = 128, timeout: int = 180):
        self._redis: Union[StrictRedis, StrictRedisCluster] = redis
        self._ip_max_conn: int = ip_max_conn
        self._timeout: int = timeout

    def start_event_handle(self) -> None:
        self.register(self.modify_max_ip_max_conn)
        self.register(self.modify_ip_max_timeout)
        self.register(self.get_info)

    def get_info(self) -> Dict[str, int]:
        return {"ip_max_conn": self._ip_max_conn, "timeout": self._timeout}

    def modify_max_ip_max_conn(self, ip_max: int) -> None:
        self._ip_max_conn = ip_max

    def modify_ip_max_timeout(self, timeout: int) -> None:
        self._timeout = timeout

    async def dispatch(self, conn: ServerConnection) -> None:
        key: str = f"{self.__class__.__name__}:{conn.peer_tuple[0]}"
        now_cnt: int = await self._redis.incr(key)
        try:
            if now_cnt > self._ip_max_conn:
                logging.error(f"Currently exceeding the maximum number of ip conn limit, close {conn.peer_tuple}")
                await Sender(conn)(
                    Response.from_event(
                        Event(Constant.EVENT_CLOSE_CONN, "Currently exceeding the maximum number of ip conn limit")
                    )
                )
                await conn.await_close()
            else:
                await self.call_next(conn)
        finally:
            await self._redis.decr(key)
            await self._redis.expire(key, self._timeout)
