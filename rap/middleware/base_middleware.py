from typing import Any, Callable, Union

from rap.common.conn import ServerConnection
from rap.common.types import BASE_REQUEST_TYPE
from rap.manager.client_manager import ClientModel
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
        ...


class BaseConnMiddleware(BaseMiddleware):
    async def dispatch(self, conn: ServerConnection):
        raise NotImplementedError


class BaseRequestMiddleware(BaseMiddleware):
    async def dispatch(self, request: BASE_REQUEST_TYPE) -> ResponseModel:
        raise NotImplementedError


class BaseMsgMiddleware(BaseMiddleware):
    async def dispatch(
        self, header: dict, call_id: int, method: Callable, param: str, client_model: "ClientModel"
    ) -> Union[dict, Exception]:
        raise NotImplementedError
