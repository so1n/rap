from rap.conn.connection import ServerConnection


class BaseConnMiddleware(object):

    async def dispatch(self, conn: ServerConnection):
        raise NotImplementedError
