from abc import ABC
from typing import TYPE_CHECKING, Any, Callable, Optional, Tuple

from rap.common.conn import ServerConnection
from rap.common.middleware import BaseMiddleware as _BaseMiddleware
from rap.server.model import RequestModel

if TYPE_CHECKING:
    from rap.server.core import Server


class BaseMiddleware(_BaseMiddleware, ABC):
    app: "Server"

    def register(self, func: Callable, name: Optional[str] = None, group: str = "middleware") -> None:
        self.app.register(func, name=name, group=group, is_private=True)

    async def start_event_handle(self) -> Any:
        pass

    async def stop_event_handle(self) -> Any:
        pass


class BaseConnMiddleware(BaseMiddleware):
    """
    Currently, the conn can be processed in the conn middleware only after the server establishes the link
    """

    async def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return await self.dispatch(*args)

    def register(self, func: Callable, name: Optional[str] = None, group: str = "conn_middleware") -> None:
        super().register(func, name, group)

    async def call_next(self, conn: ServerConnection) -> None:
        pass

    async def dispatch(self, conn: ServerConnection) -> None:
        raise NotImplementedError


class BaseMsgMiddleware(BaseMiddleware):
    def register(self, func: Callable, name: Optional[str] = None, group: str = "msg_middleware") -> None:
        super().register(func, name, group)

    async def dispatch(self, request: RequestModel, call_id: int, func: Callable, param: list) -> Tuple[int, Any]:
        raise NotImplementedError

    async def call_next(
            self, request: RequestModel, call_id: int, func: Callable, param: list
    ) -> Tuple[int, Any]:
        pass
