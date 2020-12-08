import ipaddress
from typing import List

from rap.common.conn import ServerConnection
from rap.manager.redis_manager import redis_manager
from rap.server.middleware.base import BaseConnMiddleware


class IpBlockMiddleware(BaseConnMiddleware):
    def __init__(self):
        self.register(self._add_allow_ip)
        self.register(self._add_block_ip)
        self.register(self._remove_allow_ip)
        self.register(self._remove_block_ip)
        self.register(self._get_allow_ip)
        self.register(self._get_block_ip)

        self.block_key: str = redis_manager.namespace + "block_ip"
        self.allow_key: str = redis_manager.namespace + "allow_ip"

    @staticmethod
    def ip_handle(ip: str) -> List[str]:
        ip_list: List[str] = [ip]
        if "/" in ip:
            ip_network: "ipaddress.ip_network" = ipaddress.ip_network(ip)
            ip_list = [ip for ip in ip_network.hosts()]
        return ip_list

    async def _add_allow_ip(self, ip: str):
        ip_list = self.ip_handle(ip)
        await redis_manager.redis_pool.sadd(self.allow_key, ip_list[0], ip_list[1:])

    async def _add_block_ip(self, ip: str):
        ip_list = self.ip_handle(ip)
        await redis_manager.redis_pool.sadd(self.block_key, ip_list[0], ip_list[1:])

    async def _remove_allow_ip(self, ip: str):
        ip_list = self.ip_handle(ip)
        await redis_manager.redis_pool.srem(self.allow_key, ip_list[0], ip_list[1:])

    async def _remove_block_ip(self, ip: str):
        ip_list = self.ip_handle(ip)
        await redis_manager.redis_pool.srem(self.block_key, ip_list[0], ip_list[1:])

    async def _get_allow_ip(self) -> List[str]:
        return [ip async for ip in redis_manager.redis_pool.isscan(self.allow_key)]

    async def _get_block_ip(self) -> List[str]:
        return [ip async for ip in redis_manager.redis_pool.isscan(self.block_key)]

    async def dispatch(self, conn: ServerConnection):
        ip: str = conn.peer[0]
        enable_allow: bool = await redis_manager.redis_pool.scard(self.allow_key) > 0
        if enable_allow:
            is_allow: int = await redis_manager.redis_pool.sismember(self.allow_key, ip)
            if not is_allow:
                await conn.await_close()
        else:
            is_block: int = await redis_manager.redis_pool.sismember(self.block_key, ip)
            if is_block:
                await conn.await_close()
        await self.call_next(conn)
