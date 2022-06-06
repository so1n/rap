from types import TracebackType
from typing import Optional, Type

from rap.client.model import ClientContext, Request
from rap.common.channel import UserChannel
from rap.common.context import Context as _Context
from rap.common.context import rap_context
from rap.common.utils import constant

from .base import BaseProcessor


class Context(_Context):
    request: Request
    channel: UserChannel


class ContextProcessor(BaseProcessor):
    def __init__(self) -> None:
        self._context: Context = Context()

    async def on_context_enter(self, context: ClientContext) -> None:
        context.context_token = rap_context.set({})

    async def on_context_exit(
        self,
        context: ClientContext,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        rap_context.reset(context.context_token)

    async def process_request(self, request: Request) -> Request:
        if request.msg_type is constant.MSG_REQUEST:
            self._context.request = request
        elif request.msg_type is constant.CHANNEL_REQUEST and self._context.channel is None:
            # channel can not reset token
            self._context.channel = request.context.context_channel
        return request
