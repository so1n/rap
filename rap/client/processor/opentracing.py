from typing import Optional, Tuple

from jaeger_client.span_context import SpanContext
from jaeger_client.tracer import Tracer
from opentracing import InvalidCarrierException, SpanContextCorruptedException
from opentracing.ext import tags
from opentracing.propagation import Format
from opentracing.scope import Scope

from rap.client.model import BaseMsgProtocol, Request, Response
from rap.common.utils import constant

from .base import BaseProcessor


class TracingProcessor(BaseProcessor):
    def __init__(self, tracer: Tracer, scope_cache_timeout: Optional[float] = None):
        self._tracer: Tracer = tracer
        self._scope_cache_timeout: float = scope_cache_timeout or 60.0

    def _create_scope(self, msg: BaseMsgProtocol, finish_on_close: bool = True) -> Scope:
        span_ctx: Optional[SpanContext] = None
        try:
            span_ctx = self._tracer.extract(Format.HTTP_HEADERS, msg.header)
        except (InvalidCarrierException, SpanContextCorruptedException):
            pass

        # The client sending coroutine and receiving coroutine are not the same coroutine
        scope: Scope = self._tracer.start_active_span(
            str(msg.target), child_of=span_ctx, finish_on_close=finish_on_close
        )
        self._tracer.inject(span_context=scope.span.context, format=Format.HTTP_HEADERS, carrier=msg.header)
        scope.span.set_tag(tags.SPAN_KIND, tags.SPAN_KIND_RPC_CLIENT)
        scope.span.set_tag(tags.PEER_SERVICE, self.app.server_name)
        scope.span.set_tag(tags.PEER_HOSTNAME, ":".join([str(i) for i in msg.header["host"]]))
        scope.span.set_tag("correlation_id", msg.correlation_id)
        scope.span.set_tag("msg_type", msg.msg_type)
        if not finish_on_close:
            scope.close()
        return scope

    async def process_request(self, request: Request) -> Request:
        if request.msg_type is constant.MSG_REQUEST and not request.state.get_value("scope", None):
            request.state.scope = self._create_scope(request)
        elif request.msg_type is constant.CHANNEL_REQUEST and not request.state.get_value("span", None):
            # A channel is a continuous activity that may involve the interaction of multiple coroutines
            request.state.span = self._create_scope(request, finish_on_close=False).span
            request.state.user_channel.add_done_callback(lambda f: request.state.span.finish())
        return request

    async def process_response(self, response: Response) -> Response:
        if response.msg_type is constant.MSG_RESPONSE:
            scope: Scope = response.state.scope
            status_code: int = response.status_code
            scope.span.set_tag("status_code", status_code)
            scope.span.set_tag(tags.ERROR, status_code >= 400)
            scope.close()
        return response

    async def process_exc(self, response: Response, exc: Exception) -> Tuple[Response, Exception]:
        scope: Optional[Scope] = response.state.get_value("scope", None)
        if scope and response.msg_type is constant.MSG_RESPONSE:
            status_code: int = response.status_code
            scope.span.set_tag("status_code", status_code)
            scope.span._on_error(scope.span, type(exc), exc, response.tb)
            scope.close()
        return response, exc
