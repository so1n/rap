from types import TracebackType
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Type

from rap.server.model import Request, Response, ServerContext

if TYPE_CHECKING:
    from rap.common.utils import EventEnum
    from rap.server.core import Server
    from rap.server.types import SERVER_EVENT_FN


def belong_to_base_method(func: Callable) -> bool:
    return getattr(func, "__module__", "") == __name__


class BaseProcessor(object):
    """
    feat: Process the data of a certain process (usually used to read data and write data)
    ps: If you need to share data, please use `request.stats` and `response.stats`
    """

    app: "Server"
    server_event_dict: Dict["EventEnum", List["SERVER_EVENT_FN"]] = {}

    def register(self, func: Callable, name: Optional[str] = None, group: Optional[str] = None) -> None:
        if not group:
            group = self.__class__.__name__
        if not name:
            name = func.__name__.strip("_")
        self.app.register(func, name=name, group=group, is_private=True)

    async def on_context_enter(self, context: ServerContext) -> None:
        pass

    async def on_context_exit(
        self,
        context: ServerContext,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        pass

    async def process_request(self, request: Request) -> Request:
        return request

    async def process_response(self, response: Response) -> Response:
        return response

    async def process_exc(self, response: Response) -> Response:
        return response
