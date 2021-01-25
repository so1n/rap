import logging
import time
from typing import Dict

from rap.common.conn import ServerConnection
from rap.common.utlis import Constant, Event
from rap.manager.redis_manager import redis_manager
from rap.server.middleware.base import BaseConnMiddleware
from rap.server.model import ResponseModel
from rap.server.response import Response


class ConnLimitMiddleware(BaseConnMiddleware):
    """
    feat: limit server max conn num
    """

    def __init__(self, max_conn: int = 1024, block_time: int = 30):
        self._max_conn: int = max_conn
        self._conn_count: int = 0
        self._block_time: int = block_time
        self._release_timestamp: int = int(time.time())

    def start_event_handle(self):
        self.register(self.get_conn_limit_info, group="conn_limit")
        self.register(self.modify_max_conn, group="conn_limit")
        self.register(self.modify_release_timestamp, group="conn_limit")

    def get_conn_limit_info(self) -> Dict[str, int]:
        return {
            "release_timestamp": self._release_timestamp,
            "conn_count": self._conn_count,
            "max_conn": self._max_conn,
        }

    def modify_release_timestamp(self, timestamp: int) -> None:
        self._release_timestamp = timestamp

    def modify_max_conn(self, max_conn: int) -> None:
        self._max_conn = max_conn

    async def dispatch(self, conn: ServerConnection):
        now_timestamp: int = int(time.time())
        if self._release_timestamp > now_timestamp or self._conn_count > self._max_conn:
            self._release_timestamp = now_timestamp + self._block_time
            await Response(conn)(
                ResponseModel(
                    body=Event(Constant.EVENT_CLOSE_CONN, "Currently exceeding the maximum number of connections limit")
                ),
            )
            await conn.await_close()
            return
        else:
            try:
                self._conn_count += 1
                await self.call_next(conn)
            finally:
                self._conn_count -= 1


class IpMaxConnMiddleware(BaseConnMiddleware):
    """
    feat: Limit the number of connections of a specified IP within a unit time
    """

    def __init__(self, ip_max_conn: int = 128, timeout: int = 180):
        self._ip_max_conn: int = ip_max_conn
        self._timeout: int = timeout

    def start_event_handle(self):
        self.register(self.modify_max_ip_max_conn, group="ip_max_conn")
        self.register(self.modify_ip_max_timeout, group="ip_max_conn")

    def modify_max_ip_max_conn(self, ip_max: int) -> None:
        self._ip_max_conn = ip_max

    def modify_ip_max_timeout(self, timeout: int) -> None:
        self._timeout = timeout

    async def dispatch(self, conn: ServerConnection):
        key: str = redis_manager.namespace + conn.peer_tuple[0]
        if (await redis_manager.redis_pool.get(key)) > self._ip_max_conn:
            logging.error(f"Currently exceeding the maximum number of ip conn limit, close {conn.peer_tuple}")
            await Response(conn)(
                ResponseModel(
                    body=Event(Constant.EVENT_CLOSE_CONN, "Currently exceeding the maximum number of ip conn limit")
                ),
            )
            await conn.await_close()
            return
        else:
            await redis_manager.redis_pool.incr(key)
            try:
                await self.call_next(conn)
            finally:
                await redis_manager.redis_pool.decr(key)
                await redis_manager.redis_pool.expire(key, self._timeout)
