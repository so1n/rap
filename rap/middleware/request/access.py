import logging

from rap.middleware.base_middleware import BaseRequestMiddleware
from rap.server.requests import RequestModel
from rap.server.response import ResponseModel


class AccessMiddleware(BaseRequestMiddleware):
    async def dispatch(self, request: RequestModel) -> ResponseModel:
        logging.debug(f"get request data:%s from %s", request, request.conn.peer)
        return await self.call_next(request)
