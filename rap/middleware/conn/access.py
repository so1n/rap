import logging

from rap.common.conn import ServerConnection
from rap.middleware.base_middleware import BaseConnMiddleware
from rap.server.response import ResponseModel, response


class AccessConnMiddleware(BaseConnMiddleware):
    def __init__(self, max_conn: int = 1024):
        self._max_conn: int = max_conn
        self._conn_count: int = 0

    async def dispatch(self, conn: ServerConnection):
        self._conn_count += 1
        try:
            if self._conn_count > self._max_conn:
                logging.error(f"Currently exceeding the maximum number of connections limit, close {conn.peer}")
                await response(
                    conn,
                    ResponseModel(event=("close conn", "Currently exceeding the maximum number of connections limit")),
                )
                await conn.await_close()
            else:
                logging.info(f"new conn:{conn.peer}")
                await self.call_next(conn)
        finally:
            self._conn_count -= 1
