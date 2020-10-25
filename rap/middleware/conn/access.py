import logging
from rap.conn.connection import ServerConnection

from rap.middleware.base_middleware import BaseConnMiddleware


class AccessConnMiddleware(BaseConnMiddleware):

    async def dispatch(self, conn: ServerConnection):
        logging.info(f'new conn:{conn.peer}')
        await self.call_next(conn)
