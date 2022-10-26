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
        self, context_exit_cb: ContextExitCallable
    ) -> Tuple[ClientContext, Optional[Type[BaseException]], Optional[BaseException], Optional[TracebackType]]:
        context, exc_type, exc_val, exc_tb = await context_exit_cb()
        rap_context.reset(context.context_token)
        return context, exc_type, exc_val, exc_tb

    async def process_request(self, request: Request) -> Request:
        if request.msg_type is constant.MSG_REQUEST:
            self._context.request = request
        elif request.msg_type is constant.CHANNEL_REQUEST and self._context.channel is None:
            # channel can not reset token
            self._context.channel = request.context.context_channel
        return await super().process_request(request)
