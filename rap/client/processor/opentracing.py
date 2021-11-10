from typing import Optional

from jaeger_client.span import Span
from jaeger_client.span_context import SpanContext
from jaeger_client.tracer import Tracer
from opentracing import InvalidCarrierException, SpanContextCorruptedException
from opentracing.ext import tags
from opentracing.propagation import Format
from opentracing.scope import Scope

from rap.client.model import BaseMsgProtocol, Request, Response
from rap.common.utils import Constant

from .base import BaseProcessor


class TracingProcessor(BaseProcessor):
    def __init__(self, tracer: Tracer, scope_cache_timeout: Optional[float] = None):
        self._tracer: Tracer = tracer
        self._scope_cache_timeout: float = scope_cache_timeout or 60.0

    def _get_span(self, msg: BaseMsgProtocol) -> Span:
        span_ctx: Optional[SpanContext] = None
        try:
            span_ctx = self._tracer.extract(Format.HTTP_HEADERS, msg.header)
        except (InvalidCarrierException, SpanContextCorruptedException):
            pass

        # The client sending coroutine and receiving coroutine are not the same coroutine
        scope: Scope = self._tracer.start_active_span(str(msg.target), child_of=span_ctx, finish_on_close=False)
        self._tracer.inject(span_context=scope.span.context, format=Format.HTTP_HEADERS, carrier=msg.header)
        scope.span.set_tag(tags.SPAN_KIND, tags.SPAN_KIND_RPC_CLIENT)
        scope.span.set_tag(tags.PEER_SERVICE, self.app.server_name)
        scope.span.set_tag(tags.PEER_HOSTNAME, ":".join([str(i) for i in msg.header["host"]]))
        scope.span.set_tag("correlation_id", msg.correlation_id)
        scope.span.set_tag("msg_type", msg.msg_type)
        scope.close()
        return scope.span

    async def process_request(self, request: Request) -> Request:
        if not request.state.get_value("span", None) and request.msg_type in (
            Constant.MSG_REQUEST,
            Constant.CHANNEL_REQUEST,
        ):
            request.state.span = self._get_span(request)
        return request

    async def process_response(self, response: Response) -> Response:
        if response.msg_type is Constant.MSG_RESPONSE:
            span: Span = response.state.span
            status_code: int = response.status_code
            span.set_tag("status_code", status_code)
            span.set_tag(tags.ERROR, status_code != 200)
            span.finish()
        elif (
            response.msg_type is Constant.CHANNEL_RESPONSE
            and response.header.get("channel_life_cycle") == Constant.DROP
        ):
            response.state.span.finish()
        return response
