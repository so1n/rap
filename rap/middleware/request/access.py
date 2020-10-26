import logging

from rap.middleware.base_middleware import BaseRequestMiddleware
from rap.server.requests import ResultModel, RequestModel


class AccessMiddleware(BaseRequestMiddleware):

    async def dispatch(self, request: RequestModel) -> ResultModel:
        logging.debug(f'get request data:{request} from {request.conn.peer}')
        return await self.call_next(request)
