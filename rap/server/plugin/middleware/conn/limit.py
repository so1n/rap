import logging
import time
from typing import TYPE_CHECKING, Dict, List, Optional, Union

from aredis import StrictRedis, StrictRedisCluster

from rap.common.conn import ServerConnection
from rap.common.event import CloseConnEvent
from rap.common.utils import EventEnum
from rap.server.plugin.middleware.base import BaseConnMiddleware
from rap.server.sender import Sender

if TYPE_CHECKING:
    from rap.server.core import Server
    from rap.server.types import SERVER_EVENT_FN


class ConnLimitMiddleware(BaseConnMiddleware):
    """
    feat: limit server max conn num
    """

    def __init__(self, max_conn: int = 1024, block_time: int = 30):
        self._max_conn: int = max_conn
        self._conn_count: int = 0
        self._block_time: int = block_time
        self._release_timestamp: int = int(time.time())
        self.server_event_dict: Dict[EventEnum, List["SERVER_EVENT_FN"]] = {
            EventEnum.before_start: [self.start_event_handle]
        }

    def start_event_handle(self, app: "Server") -> None:
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
                if now_timestamp > self._release_timestamp:
                    self._release_timestamp = now_timestamp + self._block_time

                await Sender(self.app, conn).send_event(
                    CloseConnEvent("Currently exceeding the maximum number of connections limit")
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

    def __init__(
        self,
        redis: Union[StrictRedis, StrictRedisCluster],
        ip_max_conn: Optional[int] = None,
        timeout: Optional[int] = None,
        namespace: str = "rap",
    ):
        """
        redis: aredis.StrictRedis and aredis.StrictRedisCluster
        ip_max_conn: Maximum number of connections per ip
        timeout:  Cache expiration time of each ip
        """
        self._redis: Union[StrictRedis, StrictRedisCluster] = redis
        self._ip_max_conn: int = ip_max_conn or 128
        self._timeout: int = timeout or 180
        self._key: str = self.__class__.__name__
        if namespace:
            self._key = f"{namespace}:{self._key}"

    def start_event_handle(self, app: "Server") -> None:
        async def _add_data_to_state(state_dict: dict) -> None:
            key: str = self.__class__.__name__
            state_dict[f"{key}:conn_cnt"] = int(await self._redis.get(key))
            state_dict[f"{key}:max_limit_cnt"] = self._ip_max_conn

        if self.app.window_state:
            self.app.window_state.add_priority_callback(_add_data_to_state)
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
        key: str = f"{self._key}:{conn.peer_tuple[0]}"
        now_cnt: int = await self._redis.incr(key)
        try:
            if now_cnt > self._ip_max_conn:
                msg: str = f"Currently exceeding the maximum number of ip conn limit, close {conn.peer_tuple}"
                logging.error(msg)
                await Sender(self.app, conn).send_event(CloseConnEvent(msg))
                await conn.await_close()
            else:
                await self.call_next(conn)
        finally:
            async with await self._redis.pipeline() as pipe:
                await pipe.decr(key)
                await pipe.expire(key, self._timeout)
                await pipe.execute()
