from typing import Set
from rap.conn.connection import ServerConnection
from rap.manager.func_manager import func_manager

from rap.middleware.base_middleware import BaseConnMiddleware


class IpBlockMiddleware(BaseConnMiddleware):
    def __init__(self):
        self.block_set: Set[str] = set()
        self.allow_set: Set[str] = set()

        func_manager.register(self._add_allow_ip, '_root_add_allow_ip')
        func_manager.register(self._add_block_ip, '_root_add_block_ip')
        func_manager.register(self._remove_allow_ip, '_root_remove_allow_ip')
        func_manager.register(self._remove_block_ip, '_root_remove_block_ip')
        func_manager.register(self._get_allow_ip, '_root_get_allow_ip')
        func_manager.register(self._get_block_ip, '_root_get_block_ip')

    def _add_allow_ip(self, ip: str):
        self.allow_set.add(ip)

    def _add_block_ip(self, ip: str):
        self.block_set.add(ip)

    def _remove_allow_ip(self, ip: str):
        self.allow_set.remove(ip)

    def _remove_block_ip(self, ip: str):
        self.block_set.remove(ip)

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
