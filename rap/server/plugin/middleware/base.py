from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Union

from rap.common.conn import ServerConnection

if TYPE_CHECKING:
    from rap.common.utils import EventEnum
    from rap.server.core import Server
    from rap.server.types import SERVER_EVENT_FN


class BaseMiddleware(object):
    app: "Server"
    server_event_dict: Dict["EventEnum", List["SERVER_EVENT_FN"]] = {}

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
