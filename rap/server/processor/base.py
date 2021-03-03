from typing import TYPE_CHECKING, Any, Callable, Optional

from rap.server.model import RequestModel, ResponseModel

if TYPE_CHECKING:
    from rap.server.core import Server


class BaseProcessor(object):
    """
    feat: Process the data of a certain process (usually used to read data and write data)
    ps: If you need to share data, please use `request.stats` and `response.stats`
    """

    app: "Server"

    def start_event_handle(self) -> None:
        return

    def stop_event_handle(self) -> None:
        return

    def register(self, func: Callable, name: Optional[str] = None, group: Optional[str] = None) -> None:
        if not group:
            group = self.__class__.__name__
        if not name:
            name = func.__name__.strip("_")
        self.app.register(func, name=name, group=group, is_private=True)

    async def process_request(self, request: RequestModel) -> RequestModel:
        raise NotImplementedError

    async def process_response(self, response: ResponseModel) -> ResponseModel:
        raise NotImplementedError
