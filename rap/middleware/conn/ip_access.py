import logging
from rap.conn.connection import ServerConnection

from rap.middleware.base_middleware import BaseMiddleware


class AccessConnMiddleware(BaseMiddleware):

    async def dispatch(self, conn: ServerConnection):
        logging.info(f'new conn:{conn.peer}')
        result = await self.call_next(conn)
        return result
