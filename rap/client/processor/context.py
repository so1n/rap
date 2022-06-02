from rap.client.model import Request, Response
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

    async def process_request(self, request: Request) -> Request:
        if request.msg_type is constant.MSG_REQUEST:
            request.context.context_token = rap_context.set({})
            self._context.request = request
        elif request.msg_type is constant.CHANNEL_REQUEST and self._context.channel is None:
            # channel can not reset token
            request.context.context_token = rap_context.set({})
            self._context.channel = request.context.context_channel
        return request

    async def process_response(self, response: Response) -> Response:
        if response.msg_type is constant.MSG_RESPONSE:
            rap_context.reset(response.context.context_token)
        return response
