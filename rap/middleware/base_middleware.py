from typing import Callable, Coroutine, Optional
from rap.conn.connection import ServerConnection


class BaseConnMiddleware(object):

    async def dispatch(self, conn: ServerConnection):
        raise NotImplementedError


class BaseRequestMiddleware(object):

    async def dispatch(self, conn: ServerConnection, callback: Optional[Coroutine[Callable]] = None):
        raise NotImplementedError
