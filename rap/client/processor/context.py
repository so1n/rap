from types import TracebackType
from typing import Optional, Tuple, Type

from rap.client.model import ClientContext, Request
from rap.client.processor.base import BaseClientProcessor, ContextExitCallable
from rap.common.channel import UserChannel
from rap.common.context import Context as _Context
from rap.common.context import rap_context
from rap.common.utils import constant


class Context(_Context):
    request: Request
    channel: UserChannel


class ContextProcessor(BaseClientProcessor):
    def __init__(self) -> None:
        self._context: Context = Context()

    async def on_context_enter(self, context: ClientContext) -> ClientContext:
        context.context_token = rap_context.set({})
        return await super().on_context_enter(context)

    async def on_context_exit(
        self, context_exit_cb: ContextExitCallable, context: ClientContext
    ) -> Tuple[ClientContext, Optional[Type[BaseException]], Optional[BaseException], Optional[TracebackType]]:
        context, exc_type, exc_val, exc_tb = await super().on_context_exit(context_exit_cb, context)
        rap_context.reset(context.context_token)
        return context, exc_type, exc_val, exc_tb

    async def on_request(self, request: Request, context: ClientContext) -> Request:
        if request.msg_type is constant.MT_MSG:
            self._context.request = request
        elif request.msg_type is constant.MT_CHANNEL and self._context.channel is None:
            # channel can not reset token
            self._context.channel = request.context.context_channel
        return await super().on_request(request, context)
