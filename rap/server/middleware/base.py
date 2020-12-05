from typing import Any, Callable, Union

from rap.common.conn import ServerConnection
from rap.common.types import BASE_REQUEST_TYPE, BASE_RESPONSE_TYPE
from rap.server.requests import RequestModel
from rap.server.response import ResponseModel


class BaseMiddleware(object):
    async def __call__(self, *args, **kwargs):
        return await self.dispatch(*args)

    async def dispatch(self, *args: Any):
        raise NotImplementedError

    def load_sub_middleware(self, call_next: "Union[Callable, BaseMiddleware]"):
        if isinstance(call_next, BaseMiddleware):
            self.call_next = call_next.call_next
        else:
            self.call_next = call_next

    async def call_next(self, value: Any):
        pass


class BaseConnMiddleware(BaseMiddleware):
    async def dispatch(self, conn: ServerConnection):
        raise NotImplementedError


class BaseRawRequestMiddleware(BaseMiddleware):
    async def dispatch(self, request: BASE_REQUEST_TYPE) -> ResponseModel:
        raise NotImplementedError


class BaseRequestDispatchMiddleware(BaseMiddleware):
    async def dispatch(
            self, request: RequestModel, response: ResponseModel
    ) -> ResponseModel:
        raise NotImplementedError


class BaseMsgMiddleware(BaseMiddleware):
    async def dispatch(
        self, request: RequestModel, call_id: int, func: Callable, param: str
    ) -> Union[dict, Exception]:
        raise NotImplementedError


class BaseResponseMiddleware(BaseMiddleware):
    async def dispatch(self, resp: ResponseModel) -> BASE_RESPONSE_TYPE:
        raise NotImplementedError
