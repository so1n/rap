from typing import TYPE_CHECKING, Callable, Optional

from rap.server.model import RequestModel, ResponseModel

if TYPE_CHECKING:
    from rap.server import Server


class BaseProcessor(object):
    """
    feat: Process the data of a certain process (usually used to read data and write data)
    ps: If you need to share data, please use `request.stats` and `response.stats`
    """

    app: "Server"

    def start_event_handle(self):
        pass

    def stop_event_handle(self):
        pass

    def register(self, func: Callable, name: Optional[str] = None, group: str = "processor"):
        self.app.register(func, name, group=group, is_private=True)

    async def process_request(self, request: RequestModel) -> RequestModel:
        return request

    async def process_response(self, response: ResponseModel) -> ResponseModel:
        return response
