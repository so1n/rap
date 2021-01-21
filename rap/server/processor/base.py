from typing import Callable, Coroutine, List, Union, TYPE_CHECKING

from rap.server.model import RequestModel, ResponseModel

if TYPE_CHECKING:
    from rap.server import Server


class BaseProcessor(object):
    """
    feat: Process the data of a certain process (usually used to read data and write data)
    ps: If you need to share data, please use `request.stats` and `response.stats`
    """

    start_event_list: List[Union[Callable, Coroutine]] = []
    stop_event_list: List[Union[Callable, Coroutine]] = []
    app: "Server"

    def register(self, func: Callable, group: str = "processor"):
        self.app.register(func, group=group)

    async def process_request(self, request: RequestModel) -> RequestModel:
        return request

    async def process_response(self, response: ResponseModel) -> ResponseModel:
        return response
