from typing import Optional

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

    def _create_scope(self, msg: BaseMsgProtocol) -> Scope:
        span_ctx: Optional[SpanContext] = None
        header_dict: dict = {}
        for k, v in msg.header.items():
            header_dict[k.lower()] = v
        try:
            span_ctx = self._tracer.extract(Format.HTTP_HEADERS, header_dict)
        except (InvalidCarrierException, SpanContextCorruptedException):
            pass

        scope: Scope = self._tracer.start_active_span(str(msg.target), child_of=span_ctx, finish_on_close=True)
        scope.span.set_tag(tags.SPAN_KIND, tags.SPAN_KIND_RPC_CLIENT)
        scope.span.set_tag(tags.PEER_SERVICE, self.app.server_name)
        scope.span.set_tag(tags.PEER_HOSTNAME, ":".join([str(i) for i in msg.header["host"]]))
        scope.span.set_tag("correlation_id", msg.correlation_id)
        scope.span.set_tag("msg_type", msg.msg_type)
        return scope

    async def process_request(self, request: Request) -> Request:
        scope: Scope = self._create_scope(request)
        if request.msg_type is Constant.MSG_REQUEST:
            self.app.cache.add(f"{self.__class__.__name__}:{request.correlation_id}", self._scope_cache_timeout, scope)
        else:
            scope.close()

        return request

    async def process_response(self, response: Response) -> Response:
        if response.msg_type is Constant.MSG_RESPONSE:
            scope: Scope = self.app.cache.get(f"{self.__class__.__name__}:{response.correlation_id}")
            status_code: int = response.status_code
            scope.span.set_tag("status_code", status_code)
            scope.span.set_tag(tags.ERROR, status_code == 200)
            scope.close()
        else:
            scope = self._create_scope(response)
            scope.close()
        return response
