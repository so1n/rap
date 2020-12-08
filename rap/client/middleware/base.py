from rap.client.model import Request, Response
from rap.common.middleware import BaseMiddleware as _BaseMiddleware


class BaseMiddleWare(_BaseMiddleware):
    async def dispatch(self, request: Request) -> Response:
        raise NotImplementedError
