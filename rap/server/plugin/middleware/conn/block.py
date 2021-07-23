import ipaddress
from typing import TYPE_CHECKING, Dict, List, Optional, Union

from aredis import StrictRedis, StrictRedisCluster  # type: ignore

from rap.common.conn import ServerConnection
from rap.common.event import CloseConnEvent
from rap.server.model import ServerEventEnum
from rap.server.plugin.middleware.base import BaseConnMiddleware
from rap.server.sender import Sender

if TYPE_CHECKING:
    from rap.server.core import Server
    from rap.server.types import SERVER_EVENT_FN


class IpBlockMiddleware(BaseConnMiddleware):
    """
    feat:
        1. block ip
        2. allow ip
    """

    def __init__(
        self,
        redis: Union[StrictRedis, StrictRedisCluster],
        allow_ip_list: Optional[List[str]] = None,
        block_ip_list: Optional[List[str]] = None,
        namespace: str = "",
    ):
        self._redis: Union[StrictRedis, StrictRedisCluster] = redis
        self.block_key: str = f"{self.__class__.__name__}:block_ip"
        self.allow_key: str = f"{self.__class__.__name__}:allow_ip"
        self.block_cnt: int = 0
        if namespace:
            self.block_key = f"{namespace}:{self.block_key}"
            self.allow_key = f"{namespace}:{self.allow_key}"

        self._allow_ip_list: List[str] = allow_ip_list if allow_ip_list else []
        self._block_ip_list: List[str] = block_ip_list if block_ip_list else []
        self.server_event_dict: Dict[ServerEventEnum, List["SERVER_EVENT_FN"]] = {
            ServerEventEnum.before_start: [self.start_event_handle]
        }

    async def start_event_handle(self, app: "Server") -> None:
        async def _add_data_to_state(state_dict: dict) -> None:
            state_dict[f"{self.__class__.__name__}:block_cnt"] = self.block_cnt

        if self.app.window_state:
            self.app.window_state.add_priority_callback(_add_data_to_state)
        self.register(self._add_allow_ip)
        self.register(self._add_block_ip)
        self.register(self._remove_allow_ip)
        self.register(self._remove_block_ip)
        self.register(self._get_allow_ip)
        self.register(self._get_block_ip)
        if self._allow_ip_list:
            await self._add_allow_ip(self._allow_ip_list)
        if self._block_ip_list:
            await self._add_block_ip(self._block_ip_list)

    @staticmethod
    def ip_network_handle(ip: str) -> List[str]:
        """
        >>> IpBlockMiddleware.ip_network_handle('192.168.0.0/31')
        ['192.168.0.1', '192.168.0.2']
        """
        ip_list: List[str] = [ip]
        if "/" in ip:
            ip_network: Union[ipaddress.IPv4Network, ipaddress.IPv6Network] = ipaddress.ip_network(ip)
            ip_list = [str(ip) for ip in ip_network.hosts()]
        return ip_list

    def ip_handle(self, ip: Union[str, List]) -> List[str]:
        ip_list: List[str] = []
        if isinstance(ip, str):
            ip = [ip]
        for _ip in ip:
            ip_list.extend(self.ip_network_handle(_ip))
        return ip_list

    async def _add_allow_ip(self, ip: Union[str, List]) -> None:
        ip_list: List[str] = self.ip_handle(ip)
        await self._redis.sadd(self.allow_key, ip_list[0], *ip_list[1:])
        await self._redis.srem(self.block_key, ip_list[0], *ip_list[1:])

    async def _add_block_ip(self, ip: Union[str, List]) -> None:
        ip_list: List[str] = self.ip_handle(ip)
        await self._redis.sadd(self.block_key, ip_list[0], *ip_list[1:])
        await self._redis.srem(self.allow_key, ip_list[0], *ip_list[1:])

    async def _remove_allow_ip(self, ip: Union[str, List]) -> None:
        ip_list: List[str] = self.ip_handle(ip)
        await self._redis.srem(self.allow_key, ip_list[0], *ip_list[1:])

    async def _remove_block_ip(self, ip: Union[str, List]) -> None:
        ip_list: List[str] = self.ip_handle(ip)
        await self._redis.srem(self.block_key, ip_list[0], *ip_list[1:])

    async def _get_allow_ip(self) -> List[str]:
        ip_list: List[str] = []
        async for ip in self._redis.sscan_iter(self.allow_key):
            ip_list.append(ip.decode())
        return ip_list

    async def _get_block_ip(self) -> List[str]:
        ip_list: List[str] = []
        async for ip in self._redis.sscan_iter(self.block_key):
            ip_list.append(ip.decode())
        return ip_list

    async def dispatch(self, conn: ServerConnection) -> None:
        if conn.peer_tuple:
            ip: str = conn.peer_tuple[0]
            enable_allow: bool = await self._redis.scard(self.allow_key) > 0
            if enable_allow:
                is_allow: int = await self._redis.sismember(self.allow_key, ip)
                if not is_allow:
                    self.block_cnt += 1
                    await Sender(conn).send_event(CloseConnEvent("not allowed to access"))
                    await conn.await_close()
                    return
            else:
                is_block: int = await self._redis.sismember(self.block_key, ip)
                if is_block:
                    self.block_cnt += 1
                    await Sender(conn).send_event(CloseConnEvent("not allowed to access"))
                    await conn.await_close()
                    return
        await self.call_next(conn)
