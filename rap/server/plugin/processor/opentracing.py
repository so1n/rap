from typing import Optional

from jaeger_client.span_context import SpanContext
from jaeger_client.tracer import Tracer
from opentracing import InvalidCarrierException, SpanContextCorruptedException
from opentracing.ext import tags
from opentracing.propagation import Format
from opentracing.scope import Scope

from rap.common.utils import constant
from rap.server.model import Request, Response, ServerMsgProtocol
from rap.server.plugin.processor.base import BaseProcessor


class TracingProcessor(BaseProcessor):
    def __init__(self, tracer: Tracer, scope_cache_timeout: Optional[float] = None):
        self._tracer: Tracer = tracer
        self._scope_cache_timeout: float = scope_cache_timeout or 60.0

    def _create_scope(self, msg: ServerMsgProtocol, finish_on_close: bool = True) -> Scope:
        span_ctx: Optional[SpanContext] = None
        try:
            span_ctx = self._tracer.extract(Format.HTTP_HEADERS, msg.header)
        except (InvalidCarrierException, SpanContextCorruptedException):
            pass

        scope = self._tracer.start_active_span(str(msg.target), child_of=span_ctx, finish_on_close=finish_on_close)
        self._tracer.inject(
            span_context=scope.span.context, format=Format.HTTP_HEADERS, carrier=msg.header  # type: ignore
        )
        scope.span.set_tag(tags.SPAN_KIND, tags.SPAN_KIND_RPC_SERVER)
        scope.span.set_tag(tags.PEER_HOSTNAME, ":".join([str(i) for i in msg.context.server_info["host"]]))
        scope.span.set_tag("correlation_id", msg.correlation_id)
        scope.span.set_tag("msg_type", msg.msg_type)
        if not finish_on_close:
            scope.close()
        return scope

    async def process_request(self, request: Request) -> Request:
        if request.msg_type is constant.MSG_REQUEST and not request.context.get_value("scope", None):
            request.context.scope = self._create_scope(request)
        elif request.msg_type is constant.CHANNEL_REQUEST and not request.context.get_value("span", None):
            # A channel is a continuous activity that may involve the interaction of multiple coroutines
            request.context.span = self._create_scope(request, finish_on_close=False).span
        return request

    async def process_response(self, response: Response) -> Response:
        if response.msg_type is constant.MSG_RESPONSE:
            scope: Scope = response.context.scope
            status_code: int = response.status_code
            scope.span.set_tag("status_code", status_code)
            scope.span.set_tag(tags.ERROR, status_code >= 400)
            scope.close()
        elif (
            response.msg_type is constant.CHANNEL_RESPONSE
            and response.header.get("channel_life_cycle", "error") == constant.DECLARE
        ):
            # The channel is created after receiving the request
            response.context.context_channel.add_done_callback(lambda f: response.context.span.finish())
        return response

    async def process_exc(self, response: Response) -> Response:
        scope: Optional[Scope] = response.context.get_value("scope", None)
        if scope and response.msg_type is constant.MSG_RESPONSE:
            status_code: int = response.status_code
            scope.span.set_tag("status_code", status_code)
            scope.span._on_error(scope.span, type(response.exc), response.exc, response.tb)  # type: ignore
            scope.close()
        return response
