import logging
from collections import defaultdict
from typing import Dict

from rap.common.conn import ServerConnection
from rap.server.middleware.base import BaseConnMiddleware
from rap.server.response import ResponseModel, Response


class ConnLimitMiddleware(BaseConnMiddleware):
    def __init__(self, max_conn: int = 1024, ip_max_conn: int = 128):
        self._max_conn: int = max_conn
        self._ip_max_conn: int = ip_max_conn
        self._conn_count: int = 0
        self._ip_count_dict: Dict[str, int] = defaultdict(lambda: 0)

    async def dispatch(self, conn: ServerConnection):
        self._conn_count += 1
        ip: str = conn.peer[0]
        self._ip_count_dict[ip] += 1
        try:
            if self._conn_count > self._max_conn:
                logging.error(f"Currently exceeding the maximum number of connections limit, close {conn.peer}")
                await Response()(
                    conn,
                    ResponseModel(event=("close conn", "Currently exceeding the maximum number of connections limit")),
                )
                await conn.await_close()
            if self._ip_count_dict[ip] > self._ip_max_conn:
                logging.error(f"Currently exceeding the maximum number of ip conn limit, close {conn.peer}")
                await Response()(
                    conn,
                    ResponseModel(event=("close conn", "Currently exceeding the maximum number of ip conn limit")),
                )
                await conn.await_close()
            else:
                logging.info(f"new conn:{conn.peer}")
                await self.call_next(conn)
        finally:
            self._conn_count -= 1
            self._ip_count_dict[ip] -= 1
