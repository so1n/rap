from rap.middleware.base_middleware import BaseRequestMiddleware
from rap.server.requests import ResultModel
from rap.common.types import BASE_REQUEST_TYPE


class AccessMiddleware(BaseRequestMiddleware):

    async def dispatch(self, request: BASE_REQUEST_TYPE) -> ResultModel:
        return self.call_next(request)
