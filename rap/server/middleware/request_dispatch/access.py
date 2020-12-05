import logging

from rap.server.middleware.base import BaseRequestDispatchMiddleware
from rap.server.requests import RequestModel
from rap.server.response import ResponseModel


class AccessMiddleware(BaseRequestDispatchMiddleware):
    async def dispatch(self, request: RequestModel, response: ResponseModel) -> ResponseModel:
        logging.debug(f"get request. type:%s data:%s from %s", response.response_num, request, request.conn.peer)
        return await self.call_next(request, response)
