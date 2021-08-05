from typing import Optional

from jaeger_client.span_context import SpanContext
from jaeger_client.tracer import Tracer
from opentracing import InvalidCarrierException, SpanContextCorruptedException
from opentracing.ext import tags
from opentracing.propagation import Format
from opentracing.scope import Scope

from rap.client.model import Request, Response
from rap.common.utils import Constant

from .base import BaseProcessor


class TracingProcessor(BaseProcessor):
    def __init__(self, tracer: Tracer):
        self._tracer: Tracer = tracer

    async def process_request(self, request: Request) -> Request:
        span_ctx: Optional[SpanContext] = None
        header_dict: dict = {}
        for k, v in request.header.items():
            header_dict[k.lower()] = v
        try:
            span_ctx = self._tracer.extract(Format.HTTP_HEADERS, header_dict)
        except (InvalidCarrierException, SpanContextCorruptedException):
            pass

        scope: Scope = self._tracer.start_active_span(str(request.target), child_of=span_ctx, finish_on_close=True)
        request.state.scope = scope
        scope.span.set_tag(tags.SPAN_KIND, tags.SPAN_KIND_RPC_CLIENT)
        scope.span.set_tag(tags.PEER_SERVICE, request.target)
        scope.span.set_tag(tags.PEER_HOSTNAME, ":".join([str(i) for i in request.header["host"]]))
        scope.span.set_tag("correlation_id", request.correlation_id)
        scope.span.set_tag("msg_type", request.msg_type)

        if request.msg_type is Constant.CHANNEL_REQUEST and scope:
            scope.close()
        return request

    async def process_response(self, response: Response) -> Response:
        scope: Optional[Scope] = response.state.scope
        if scope:
            status_code: int = response.status_code
            scope.span.set_tag("status_code", status_code)
            scope.span.set_tag(tags.ERROR, status_code == 200)
            scope.close()
        return response
