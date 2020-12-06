import ipaddress
from typing import List, Set, Optional

from rap.common.conn import ServerConnection
from rap.manager.func_manager import func_manager
from rap.server.middleware.base import BaseConnMiddleware


class IpBlockMiddleware(BaseConnMiddleware):
    def __init__(self, allow_set: Optional[set] = None, block_set: Optional[Set] = None):
        self.block_set: Set[str] = block_set if block_set else set()
        self.allow_set: Set[str] = allow_set if allow_set else set()

        func_manager.register(self._add_allow_ip, "_root_add_allow_ip")
        func_manager.register(self._add_block_ip, "_root_add_block_ip")
        func_manager.register(self._remove_allow_ip, "_root_remove_allow_ip")
        func_manager.register(self._remove_block_ip, "_root_remove_block_ip")
        func_manager.register(self._get_allow_ip, "_root_get_allow_ip")
        func_manager.register(self._get_block_ip, "_root_get_block_ip")
        super().__init__()

    @staticmethod
    def ip_handle(ip: str) -> List[str]:
        ip_list: List[str] = [ip]
        if '/' in ip:
            ip_network: 'ipaddress.ip_network' = ipaddress.ip_network(ip)
            ip_list = [ip for ip in ip_network.hosts()]
        return ip_list

    def _add_allow_ip(self, ip: str):
        self.allow_set.update(self.ip_handle(ip))

    def _add_block_ip(self, ip: str):
        self.block_set.update(self.ip_handle(ip))

    def _remove_allow_ip(self, ip: str):
        self.allow_set.difference(self.ip_handle(ip))

    def _remove_block_ip(self, ip: str):
        self.block_set.difference(self.ip_handle(ip))

    def _get_allow_ip(self):
        return self.allow_set

    def _get_block_ip(self):
        return self.block_set

    async def dispatch(self, conn: ServerConnection):
        ip: str = conn.peer[0]
        if self.allow_set:
            if ip not in self.allow_set:
                await conn.await_close()
        else:
            if ip in self.block_set:
                await conn.await_close()
        await self.call_next(conn)
