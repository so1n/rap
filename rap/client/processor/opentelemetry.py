from types import TracebackType
from typing import Optional, Type

from opentelemetry import context, trace
from opentelemetry.propagate import inject
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.trace.status import Status, StatusCode

from rap.client.model import ClientContext, Request, Response
from rap.common.utils import constant

from .base import BaseProcessor


class OpenTelemetryProcessor(BaseProcessor):
    def __init__(
        self,
        tracer_provider: Optional[trace.TracerProvider] = None,
    ):
        self._tracer: trace.Tracer = trace.get_tracer(
            instrumenting_module_name="rap",
            tracer_provider=tracer_provider,
        )

    async def on_context_exit(
        self,
        _context: ClientContext,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        _context.iter_span.__exit__(exc_type, exc_val, exc_tb)

    async def process_request(self, request: Request) -> Request:
        if not request.context.get_value("span", None):
            service_name, group, func_name = request.target.split("/")
            iter_span = self._tracer.start_as_current_span(
                name=request.target,
                kind=trace.SpanKind.CLIENT,
                attributes={
                    SpanAttributes.RPC_SYSTEM: "rap",
                    SpanAttributes.RPC_GRPC_STATUS_CODE: 0,
                    SpanAttributes.RPC_METHOD: func_name,
                    SpanAttributes.RPC_SERVICE: service_name + "-" + group,
                    "rap.msg_type": request.msg_type,
                    "rap.correlation_id": request.correlation_id,
                    "rap.status_code": request.header.get("status_code", 0),
                },
            )
            request.context.iter_span = iter_span
            span = iter_span.__enter__()
            inject(request.header, context=context.get_current())
            request.context.span = span
        return request

    async def process_response(self, response: Response) -> Response:
        if (response.msg_type is constant.MSG_RESPONSE) or (
            response.msg_type is constant.CHANNEL_RESPONSE
            and response.header.get("channel_life_cycle", "error") == constant.DECLARE
        ):
            span: trace.Span = response.context.span
            span.set_status(Status(status_code=StatusCode.OK))
            span.set_attribute("rap.status_code", response.status_code)
        return response
