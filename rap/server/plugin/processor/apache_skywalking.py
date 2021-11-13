from traceback import format_tb
from typing import Optional, Tuple

from skywalking import Component, Layer, Log, LogItem
from skywalking.trace.carrier import Carrier
from skywalking.trace.context import get_context
from skywalking.trace.span import Span
from skywalking.trace.tags import Tag
from skywalking.utils import filter

from rap.common.utils import Constant
from rap.server.model import BaseMsgProtocol, Request, Response

from .base import BaseProcessor

# class Component(Enum):
#     RAP = 7777


class TagCorrelationId(Tag):
    key = "rap.correlation_id"


class TagMsgType(Tag):
    key = "rap.msg_type"


class TagStatusCode(Tag):
    key = "rap.status_code"


class TagRapType(Tag):
    key = "rap.type"


class SkywalkingProcessor(BaseProcessor):
    def __init__(self, carrier_key_prefix: str = "X-Rap"):
        self._carrier_key_prefix = carrier_key_prefix

    def _create_span(self, msg: BaseMsgProtocol) -> Span:
        carrier: Carrier = Carrier()
        for item in carrier:
            key: str = self._carrier_key_prefix + "-" + item.key
            if key in msg.header:
                item.val = msg.header[key]
        span: Span = get_context().new_entry_span(op=msg.target, carrier=carrier)
        span.start()
        span.layer = Layer.RPCFramework
        span.component = Component.Unknown
        span.peer = ":".join([str(i) for i in msg.header["host"]])
        span.tag(TagRapType("server"))
        span.tag(TagCorrelationId(msg.correlation_id))
        span.tag(TagMsgType(msg.msg_type))
        return span

    async def process_request(self, request: Request) -> Request:
        if request.msg_type is Constant.MSG_REQUEST and not request.state.get_value("span", None):
            request.state.span = self._create_span(request)
        elif request.msg_type is Constant.CHANNEL_REQUEST and not request.state.get_value("span", None):
            # A channel is a continuous activity that may involve the interaction of multiple coroutines
            request.state.span = self._create_span(request)
        return request

    async def process_response(self, response: Response) -> Response:
        if response.msg_type is Constant.MSG_RESPONSE:
            span: Span = response.state.span
            status_code: int = response.status_code
            span.tag(TagStatusCode(status_code))
            span.error_occurred = status_code >= 400
            span.stop()
        elif (
            response.msg_type is Constant.CHANNEL_RESPONSE
            and response.header.get("channel_life_cycle", "error") == Constant.DECLARE
        ):
            # The channel is created after receiving the request
            response.state.user_channel.add_done_callback(lambda f: response.state.span.stop())
        return response

    async def process_exc(self, response: Response, exc: Exception) -> Tuple[Response, Exception]:
        span: Optional[Span] = response.state.get_value("span", None)
        if span:
            status_code: int = response.status_code
            span.tag(TagStatusCode(status_code))
            span.error_occurred = True
            span.logs = [
                Log(items=[LogItem(key="Traceback", val=filter.sw_filter(target="".join(format_tb(response.tb))))])
            ]
            if response.msg_type is not Constant.CHANNEL_RESPONSE:
                span.stop()
        return response, exc
