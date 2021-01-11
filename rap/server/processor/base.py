from typing import Callable, Coroutine, List, Union

from rap.manager.func_manager import func_manager
from rap.server.model import RequestModel, ResponseModel


class BaseProcessor(object):
    """
    feat: Process the data of a certain process (usually used to read data and write data)
    ps: If you need to share data, please use `request.stats` and `response.stats`
    """

    start_event_list: List[Union[Callable, Coroutine]] = []
    stop_event_list: List[Union[Callable, Coroutine]] = []

    @staticmethod
    def register(func: Callable, is_root: bool = True, group: str = "processor"):
        func_manager.register(func, is_root=is_root, group=group)

    async def process_request(self, request: RequestModel):
        pass

    async def process_response(self, response: ResponseModel):
        pass
