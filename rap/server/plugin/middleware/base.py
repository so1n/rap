from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, Tuple, Union

from rap.common.conn import ServerConnection
from rap.server.model import Request

if TYPE_CHECKING:
    from rap.server.core import Server


class BaseMiddleware(object):
    app: "Server"

    async def __call__(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError

    async def _call_next(self, *args: Any) -> Any:
        pass

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

    def load_sub_middleware(self, call_next: "Union[Callable, BaseMiddleware]") -> None:
        if isinstance(call_next, BaseMiddleware):
            setattr(self, self._call_next.__name__, call_next.__call__)
        else:
            setattr(self, self._call_next.__name__, call_next)


class BaseConnMiddleware(BaseMiddleware):
    """
    Currently, the conn can be processed in the conn middleware only after the server establishes the link
    """

    async def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return await self.dispatch(*args)

    async def call_next(self, conn: ServerConnection) -> None:
        return await self._call_next(conn)

    async def dispatch(self, conn: ServerConnection) -> None:
        raise NotImplementedError


class BaseMsgMiddleware(BaseMiddleware):
    async def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return await self.dispatch(*args)

    async def dispatch(
        self, request: Request, call_id: int, func: Callable, param: list, default_param: Dict[str, Any]
    ) -> Tuple[int, Any]:
        raise NotImplementedError

    async def call_next(
        self, request: Request, call_id: int, func: Callable, param: list, default_param: Dict[str, Any]
    ) -> Tuple[int, Any]:
        return await self._call_next(request, call_id, func, param, default_param)
