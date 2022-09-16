from types import TracebackType
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Type

from rap.client.model import ClientContext, Request, Response
from rap.client.types import CLIENT_EVENT_FN
from rap.common.utils import EventEnum

if TYPE_CHECKING:
    from rap.client.core import BaseClient


def belong_to_base_method(func: Callable) -> bool:
    """
    Determine whether the processor's method is reimplemented by itself or the method of calling the BaseProcessor
    """
    return getattr(func, "__module__", "") == __name__


class BaseProcessor(object):
    """client processor
    Note:
        It needs to be loaded before the client is started.
        The client will automatically register the corresponding event callback when it is loaded.
        After the client is started, it will assign itself to the corresponding `app` property
    """

    app: "BaseClient"
    event_dict: Dict["EventEnum", List[CLIENT_EVENT_FN]] = {}

    async def on_context_enter(self, context: ClientContext) -> None:
        pass

    async def on_context_exit(
        self,
        context: ClientContext,
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
