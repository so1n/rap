from abc import ABC
from typing import Any, Callable, Coroutine, List, Union

from rap.common.conn import ServerConnection
from rap.common.middleware import BaseMiddleware as _BaseMiddleware
from rap.manager.func_manager import func_manager
from rap.server.model import RequestModel


class BaseMiddleware(_BaseMiddleware, ABC):
    start_event_list: List[Union[Callable, Coroutine]] = []
    stop_event_list: List[Union[Callable, Coroutine]] = []

    @staticmethod
    def register(func: Callable, is_root: bool = True, group: str = "middleware"):
        func_manager.register(func, is_root=is_root, group=group)


class BaseConnMiddleware(BaseMiddleware):
    """
    Currently, the conn can be processed in the conn middleware only after the server establishes the link
    """

    async def __call__(self, *args, **kwargs):
        return await self.dispatch(*args)

    @staticmethod
    def register(func: Callable, is_root: bool = True, group: str = "conn_middleware"):
        super().register(func, is_root, group)

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
    @staticmethod
    def register(func: Callable, is_root: bool = True, group: str = "msg_middleware"):
        super().register(func, is_root, group)

    async def dispatch(self, request: RequestModel, call_id: int, func: Callable, param: str) -> Union[dict, Exception]:
        raise NotImplementedError
