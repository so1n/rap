from rap.common.channel import UserChannel
from rap.common.context import Context as _Context
from rap.common.context import rap_context
from rap.common.utils import constant
from rap.server.model import Request, Response
from rap.server.plugin.processor.base import BaseProcessor, ResponseCallable


class Context(_Context):
    request: Request
    channel: UserChannel


class ContextProcessor(BaseProcessor):
    def __init__(self) -> None:
        self._context: Context = Context()

    async def process_request(self, request: Request) -> Request:
        if request.msg_type is constant.MT_MSG:
            request.context.context_token = rap_context.set({})
            self._context.request = request
        elif request.msg_type is constant.MT_CHANNEL and not request.context.get_value("context_token", None):
            # channel can not reset token
            request.context.context_token = rap_context.set({})
        return request

    async def process_response(self, response_cb: ResponseCallable) -> Response:
        response: Response = await super().process_response(response_cb)
        if response.msg_type is constant.MT_MSG:
            rap_context.reset(response.context.context_token)
        elif (
            response.msg_type is constant.MT_CHANNEL
            and response.header.get("channel_life_cycle", "error") == constant.DECLARE
        ):
            # The channel is created after receiving the request
            self._context.channel = response.context.context_channel
        return response
