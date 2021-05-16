from abc import ABC
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, Tuple

from rap.common.conn import ServerConnection
from rap.common.middleware import BaseMiddleware as _BaseMiddleware
from rap.server.model import Request

if TYPE_CHECKING:
    from rap.server.core import Server


class BaseMiddleware(_BaseMiddleware, ABC):
    app: "Server"

    def register(self, func: Callable, name: Optional[str] = None, group: Optional[str] = None) -> None:
        if not group:
            group = self.__class__.__name__
        if not name:
            name = func.__name__.strip("_")
        self.app.register(func, name=name, group=group, is_private=True)

    def start_event_handle(self) -> Any:
        pass

    def stop_event_handle(self) -> Any:
        pass


class BaseConnMiddleware(BaseMiddleware):
    """
    Currently, the conn can be processed in the conn middleware only after the server establishes the link
    """

    async def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return await self.dispatch(*args)

    async def call_next(self, conn: ServerConnection) -> None:
        pass

    async def dispatch(self, conn: ServerConnection) -> None:
        raise NotImplementedError


class BaseMsgMiddleware(BaseMiddleware):
    async def dispatch(
        self, request: Request, call_id: int, func: Callable, param: list, default_param: Dict[str, Any]
    ) -> Tuple[int, Any]:
        raise NotImplementedError

    async def call_next(
        self, request: Request, call_id: int, func: Callable, param: list, default_param: Dict[str, Any]
    ) -> Tuple[int, Any]:
        pass
