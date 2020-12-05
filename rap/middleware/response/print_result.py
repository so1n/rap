from rap.middleware.base_middleware import BaseResponseMiddleware
from rap.common.types import BASE_RESPONSE_TYPE
from rap.server.response import ResponseModel


class PrintResultMiddleware(BaseResponseMiddleware):
    async def dispatch(self, response: ResponseModel) -> BASE_RESPONSE_TYPE:
        print(response.result)
        return await self.call_next(response)
