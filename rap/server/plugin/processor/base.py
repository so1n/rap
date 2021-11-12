from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Tuple

from rap.server.model import Request, Response

if TYPE_CHECKING:
    from rap.common.utils import EventEnum
    from rap.server.core import Server
    from rap.server.types import SERVER_EVENT_FN


class BaseProcessor(object):
    """
    feat: Process the data of a certain process (usually used to read data and write data)
    ps: If you need to share data, please use `request.stats` and `response.stats`
    """

    app: "Server"
    server_event_dict: Dict["EventEnum", List["SERVER_EVENT_FN"]] = {}

    def start_event_handle(self, app: "Server") -> None:
        pass

    def stop_event_handle(self, app: "Server") -> None:
        pass

    def register(self, func: Callable, name: Optional[str] = None, group: Optional[str] = None) -> None:
        if not group:
            group = self.__class__.__name__
        if not name:
            name = func.__name__.strip("_")
        self.app.register(func, name=name, group=group, is_private=True)

    async def process_request(self, request: Request) -> Request:
        return request

    async def process_response(self, response: Response) -> Response:
        return response

    async def process_exc(self, response: Response, exc: Exception) -> Tuple[Response, Exception]:
        return response, exc
