from traceback import format_tb
from typing import Optional, Tuple

from skywalking import Component, Layer, Log, LogItem
from skywalking.trace.carrier import Carrier
from skywalking.trace.context import get_context
from skywalking.trace.span import Span
from skywalking.trace.tags import Tag
from skywalking.utils import filter

from rap.client.model import BaseMsgProtocol, Request, Response
from rap.common.utils import Constant

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
        peer: str = ":".join([str(i) for i in msg.header["host"]])
        if carrier.is_valid:
            span: Span = get_context().new_entry_span(op=msg.target, carrier=carrier)
            span.start()
            span.peer = peer
        else:
            span = get_context().new_exit_span(op=msg.target, peer=peer)
            span.start()
            carrier = span.inject()
            for item in carrier:
                key = self._carrier_key_prefix + "-" + item.key
                msg.header[key] = item.val
        span.layer = Layer.RPCFramework
        span.component = Component.Unknown
        span.tag(TagRapType("client"))
        span.tag(TagCorrelationId(msg.correlation_id))
        span.tag(TagMsgType(msg.msg_type))
        return span

    async def process_request(self, request: Request) -> Request:
        if request.msg_type is Constant.MSG_REQUEST:
            request.state.span = self._create_span(request)
        elif request.msg_type is Constant.CHANNEL_REQUEST and not request.state.get_value("span", None):
            # A channel is a continuous activity that may involve the interaction of multiple coroutines
            request.state.span = self._create_span(request)
            request.state.user_channel.add_done_callback(lambda f: request.state.span.stop())
        return request

    async def process_response(self, response: Response) -> Response:
        if response.msg_type is Constant.MSG_RESPONSE:
            span: Span = response.state.span
            status_code: int = response.status_code
            span.tag(TagStatusCode(status_code))
            span.error_occurred = status_code >= 400
            span.stop()
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
