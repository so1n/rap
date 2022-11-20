from types import TracebackType
from typing import TYPE_CHECKING, Any, Callable, Coroutine, Dict, List, Optional, Tuple, Type, TypeVar

from rap.server.model import Request, Response, ServerContext

if TYPE_CHECKING:
    from rap.common.utils import EventEnum
    from rap.server.core import Server
    from rap.server.types import SERVER_EVENT_FN

ResponseCallable = Callable[[bool], Coroutine[Any, Any, Tuple[Response, Optional[Exception]]]]
ContextExitType = Tuple[ServerContext, Optional[Type[BaseException]], Optional[BaseException], Optional[TracebackType]]
ContextExitCallable = Callable[[], Coroutine[Any, Any, ContextExitType]]
_ProcessorT = TypeVar("_ProcessorT", bound="BaseProcessor")


def belong_to_base_method(func: Callable) -> bool:
    return getattr(func, "__module__", "") == __name__


def chain_processor(*processor_list: "_ProcessorT") -> "_ProcessorT":
    """Chain each Processor instance and return the first one"""
    for index, processor in enumerate(processor_list):
        if processor == processor_list[-1]:
            continue
        else:
            next_processor: BaseProcessor = processor_list[index + 1]
            setattr(processor, processor._next_context_enter.__name__, next_processor.on_context_enter)
            setattr(processor, processor._next_request.__name__, next_processor.on_request)
            setattr(processor, processor._next_process_response.__name__, next_processor.on_response)
            setattr(processor, processor._next_context_exit.__name__, next_processor.on_context_exit)
    return processor_list[0]


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

    async def _next_context_enter(self, context: ServerContext) -> ServerContext:
        return context

    async def on_context_enter(self, context: ServerContext) -> ServerContext:
        return await self._next_context_enter(context)

    async def _next_request(self, request: Request, context: ServerContext) -> Request:
        return request

    async def on_request(self, request: Request, context: ServerContext) -> Request:
        return await self._next_request(request, context)

    async def _next_process_response(self, response_cb: ResponseCallable, context: ServerContext) -> Response:
        return (await response_cb(True))[0]

    async def on_response(self, response_cb: ResponseCallable, context: ServerContext) -> Response:
        return await self._next_process_response(response_cb, context)

    async def _next_context_exit(self, context_exit_cb: ContextExitCallable, context: ServerContext) -> ContextExitType:
        return await context_exit_cb()

    async def on_context_exit(self, context_exit_cb: ContextExitCallable, context: ServerContext) -> ContextExitType:
        return await self._next_context_exit(context_exit_cb, context)
