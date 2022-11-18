from types import TracebackType
from typing import Optional, Tuple, Type

from jaeger_client.span_context import SpanContext
from jaeger_client.tracer import Tracer
from opentracing import InvalidCarrierException, SpanContextCorruptedException
from opentracing.ext import tags
from opentracing.propagation import Format
from opentracing.scope import Scope

from rap.common.msg import BaseMsgProtocol
from rap.common.utils import constant
from rap.server.model import Request, Response, ServerContext
from rap.server.plugin.processor.base import BaseProcessor, ContextExitCallable, ResponseCallable


class TracingProcessor(BaseProcessor):
    def __init__(self, tracer: Tracer, scope_cache_timeout: Optional[float] = None):
        self._tracer: Tracer = tracer
        self._scope_cache_timeout: float = scope_cache_timeout or 60.0

    def _create_scope(self, msg: BaseMsgProtocol, finish_on_close: bool = True) -> Optional[Scope]:
        if msg.msg_type in (constant.MT_CLIENT_EVENT, constant.MT_SERVER_EVENT):
            return None
        ctx_scope: Optional[Scope] = msg.context.get_value("scope", None)
        if ctx_scope:
            return ctx_scope
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
        msg.context.scope = scope
        return scope

    async def process_request(self, request: Request) -> Request:
        self._create_scope(request)
        return request

    async def process_response(self, response_cb: ResponseCallable) -> Response:
        response: Response = await super().process_response(response_cb)
        # TODO support server push msg
        # self._create_scope(response)
        ctx_scope: Optional[Scope] = response.context.get_value("scope", None)
        if not ctx_scope:
            return response
        if response.msg_type is constant.MT_MSG:
            scope: Scope = response.context.scope
            status_code: int = response.status_code
            scope.span.set_tag("status_code", status_code)
            scope.span.set_tag(tags.ERROR, status_code >= 400)
            scope.close()
        elif (
            response.msg_type is constant.MT_CHANNEL
            and response.header.get("channel_life_cycle", "error") == constant.DECLARE
        ):
            # The channel is created after receiving the request
            response.context.context_channel.add_done_callback(lambda f: response.context.span.finish())
        return response

    async def on_context_exit(
        self, context_exit_cb: ContextExitCallable
    ) -> Tuple[ServerContext, Optional[Type[BaseException]], Optional[BaseException], Optional[TracebackType]]:
        context, exc_type, exc_val, exc_tb = await super().on_context_exit(context_exit_cb)
        scope: Optional[Scope] = context.get_value("scope", None)
        if scope:
            scope.__exit__(exc_type, exc_val, exc_tb)
        return context, exc_type, exc_val, exc_tb
