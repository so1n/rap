from types import TracebackType
from typing import TYPE_CHECKING, Any, Callable, Coroutine, Dict, List, Optional, Tuple, Type, TypeVar

from rap.client.model import ClientContext, Request, Response
from rap.client.types import CLIENT_EVENT_FN
from rap.common.utils import EventEnum

if TYPE_CHECKING:
    from rap.client.core import BaseClient


ResponseCallable = Callable[[bool], Coroutine[Any, Any, Tuple[Response, Optional[Exception]]]]
ContextExitType = Tuple[ClientContext, Optional[Type[BaseException]], Optional[BaseException], Optional[TracebackType]]
ContextExitCallable = Callable[[], Coroutine[Any, Any, ContextExitType]]
_ProcessorT = TypeVar("_ProcessorT", bound="BaseProcessor")


def belong_to_base_method(func: Callable) -> bool:
    """
    Determine whether the processor's method is reimplemented by itself or the method of calling the BaseProcessor
    """
    return getattr(func, "__module__", "") == __name__


def chain_processor(*processor_list: "_ProcessorT") -> "_ProcessorT":
    """Chain each Processor instance and return the first one"""
    for index, processor in enumerate(processor_list):
        if processor == processor_list[-1]:
            continue
        else:
            next_processor: BaseProcessor = processor_list[index + 1]
            setattr(processor, processor.next_context_enter.__name__, next_processor.on_context_enter)
            setattr(processor, processor.next_process_request.__name__, next_processor.process_request)
            setattr(processor, processor.next_process_response.__name__, next_processor.process_response)
            setattr(processor, processor.next_context_exit.__name__, next_processor.on_context_exit)
    return processor_list[0]


class BaseProcessor(object):
    """
    Transport processor
     This class can only be used by the `Transport` class
    """

    async def next_context_enter(self, context: ClientContext) -> ClientContext:
        return context

    async def on_context_enter(self, context: ClientContext) -> ClientContext:
        return await self.next_context_enter(context)

    async def next_process_request(self, request: Request) -> Request:
        return request

    async def process_request(self, request: Request) -> Request:
        return await self.next_process_request(request)

    async def next_process_response(self, response_cb: ResponseCallable) -> Response:
        return (await response_cb(True))[0]

    async def process_response(self, response_cb: ResponseCallable) -> Response:
        return await self.next_process_response(response_cb)

    async def next_context_exit(self, context_exit_cb: ContextExitCallable) -> ContextExitType:
        return await context_exit_cb()

    async def on_context_exit(self, context_exit_cb: ContextExitCallable) -> ContextExitType:
        return await self.next_context_exit(context_exit_cb)


class BaseClientProcessor(BaseProcessor):
    """processor
    Note:
        It needs to be loaded before the client is started.
        The client will automatically register the corresponding event callback when it is loaded.
        After the client is started, it will assign itself to the corresponding `app` property
    """

    app: "BaseClient"
    event_dict: Dict["EventEnum", List[CLIENT_EVENT_FN]] = {}
