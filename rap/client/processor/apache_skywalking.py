from types import TracebackType
from typing import Optional, Tuple, Type

from skywalking import Component, Layer
from skywalking.trace.carrier import Carrier
from skywalking.trace.context import get_context
from skywalking.trace.span import Span
from skywalking.trace.tags import Tag

from rap.client.model import BaseMsgProtocol, ClientContext, Request, Response
from rap.common.utils import constant

from .base import BaseClientProcessor

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


class SkywalkingProcessor(BaseClientProcessor):
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
        try:
            if request.msg_type is constant.MSG_REQUEST:
                request.context.span = self._create_span(request)
            elif request.msg_type is constant.CHANNEL_REQUEST and not request.context.get_value("span", None):
                # A channel is a continuous activity that may involve the interaction of multiple coroutines
                request.context.span = self._create_span(request)
        except Exception:
            pass
        return await super().process_request(request)

    async def process_response(self, response: Response) -> Response:
        if response.msg_type is constant.MSG_RESPONSE:
            span: Span = response.context.span
            status_code: int = response.status_code
            span.tag(TagStatusCode(status_code))
            span.error_occurred = status_code >= 400
            span.stop()
        return await super().process_response(response)

    async def on_context_exit(
        self,
        context: ClientContext,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> Tuple[ClientContext, Optional[Type[BaseException]], Optional[BaseException], Optional[TracebackType]]:
        span: Optional[Span] = context.get_value("span", None)
        if span:
            span.__exit__(exc_type, exc_val, exc_tb)
        return await super().on_context_exit(context, exc_type, exc_val, exc_tb)
