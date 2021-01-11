import ipaddress
from typing import List, Union

from rap.common.conn import ServerConnection
from rap.manager.redis_manager import redis_manager
from rap.server.middleware.base import BaseConnMiddleware


class IpBlockMiddleware(BaseConnMiddleware):
    """
    feat:
        1. block ip
        2. allow ip
    """

    def __init__(self, allow_ip_list: List[str] = None, block_ip_list: List[str] = None):
        self.register(self._add_allow_ip)
        self.register(self._add_block_ip)
        self.register(self._remove_allow_ip)
        self.register(self._remove_block_ip)
        self.register(self._get_allow_ip)
        self.register(self._get_block_ip)

        self.block_key: str = redis_manager.namespace + "block_ip"
        self.allow_key: str = redis_manager.namespace + "allow_ip"

        if allow_ip_list:
            self.start_event_list.append(self._add_allow_ip(allow_ip_list))
        if block_ip_list:
            self.start_event_list.append(self._add_block_ip(block_ip_list))

    @staticmethod
    def ip_network_handle(ip: str) -> List[str]:
        """
        >>> IpBlockMiddleware.ip_network_handle('192.168.0.0/31')
        ['192.168.0.1', '192.168.0.2']
        """
        ip_list: List[str] = [ip]
        if "/" in ip:
            ip_network: "ipaddress.ip_network" = ipaddress.ip_network(ip)
            ip_list = [str(ip) for ip in ip_network.hosts()]
        return ip_list

    def ip_handle(self, ip: Union[str, List]) -> List[str]:
        ip_list: List[str] = []
        if type(ip) is str:
            ip = [ip]
            for _ip in ip:
                ip_list.extend(self.ip_network_handle(_ip))
        return ip_list

    async def _add_allow_ip(self, ip: Union[str, List]):
        ip_list = self.ip_handle(ip)
        await redis_manager.redis_pool.sadd(self.allow_key, ip_list[0], ip_list[1:])
        await redis_manager.redis_pool.srem(self.block_key, ip_list[0], ip_list[1:])

    async def _add_block_ip(self, ip: Union[str, List]):
        ip_list = self.ip_handle(ip)
        await redis_manager.redis_pool.sadd(self.block_key, ip_list[0], ip_list[1:])
        await redis_manager.redis_pool.srem(self.allow_key, ip_list[0], ip_list[1:])

    async def _remove_allow_ip(self, ip: Union[str, List]):
        ip_list = self.ip_handle(ip)
        await redis_manager.redis_pool.srem(self.allow_key, ip_list[0], ip_list[1:])

    async def _remove_block_ip(self, ip: Union[str, List]):
        ip_list = self.ip_handle(ip)
        await redis_manager.redis_pool.srem(self.block_key, ip_list[0], ip_list[1:])

    async def _get_allow_ip(self) -> List[str]:
        return [ip async for ip in redis_manager.redis_pool.isscan(self.allow_key)]

    async def _get_block_ip(self) -> List[str]:
        return [ip async for ip in redis_manager.redis_pool.isscan(self.block_key)]

    async def dispatch(self, conn: ServerConnection):
        ip: str = conn.peer_tuple[0]
        enable_allow: bool = await redis_manager.redis_pool.scard(self.allow_key) > 0
        if enable_allow:
            is_allow: int = await redis_manager.redis_pool.sismember(self.allow_key, ip)
            if not is_allow:
                await conn.await_close()
                return
        else:
            is_block: int = await redis_manager.redis_pool.sismember(self.block_key, ip)
            if is_block:
                await conn.await_close()
                return
        await self.call_next(conn)
