from abc import ABC
from typing import Any, Callable, Union

from rap.common.conn import ServerConnection
from rap.common.middleware import BaseMiddleware as _BaseMiddleware
from rap.manager.func_manager import func_manager
from rap.server.model import RequestModel


class BaseMiddleware(_BaseMiddleware, ABC):
    @staticmethod
    def register(func: Callable, is_root: bool = True, group: str = "processor"):
        func_manager.register(func, is_root=is_root, group=group)


class BaseConnMiddleware(BaseMiddleware):
    async def __call__(self, *args, **kwargs):
        return await self.dispatch(*args)

    @staticmethod
    def register(func: Callable, is_root: bool = True, group: str = "processor"):
        func_manager.register(func, is_root=is_root, group=group)

    def load_sub_middleware(self, call_next: "Union[Callable, BaseMiddleware]"):
        if isinstance(call_next, BaseMiddleware):
            self.call_next = call_next.call_next
        else:
            self.call_next = call_next

    async def call_next(self, value: Any):
        pass

    async def dispatch(self, conn: ServerConnection):
        raise NotImplementedError


class BaseMsgMiddleware(BaseMiddleware):
    async def dispatch(self, request: RequestModel, call_id: int, func: Callable, param: str) -> Union[dict, Exception]:
        raise NotImplementedError
