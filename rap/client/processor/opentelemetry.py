from types import TracebackType
from typing import Iterator, Optional, Tuple, Type

from opentelemetry import context, trace
from opentelemetry.propagate import inject
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.trace.span import Span
from opentelemetry.trace.status import Status, StatusCode

from rap.client.model import ClientContext, Request, Response
from rap.client.processor.base import BaseClientProcessor, ContextExitCallable, ResponseCallable
from rap.common.utils import constant


class OpenTelemetryProcessor(BaseClientProcessor):
    def __init__(
        self,
        tracer_provider: Optional[trace.TracerProvider] = None,
    ):
        self._tracer: trace.Tracer = trace.get_tracer(
            instrumenting_module_name="rap",
            tracer_provider=tracer_provider,
        )

    def _create_scope(self, msg: Request) -> Span:
        service_name, group, func_name = msg.target.split("/")
        iter_span: Iterator[Span] = self._tracer.start_as_current_span(
            name=msg.target,
            kind=trace.SpanKind.CLIENT,
            attributes={
                SpanAttributes.RPC_SYSTEM: "rap",
                SpanAttributes.RPC_GRPC_STATUS_CODE: 0,
                SpanAttributes.RPC_METHOD: func_name,
                SpanAttributes.RPC_SERVICE: service_name + "-" + group,
                "rap.msg_type": msg.msg_type,
                "rap.correlation_id": msg.correlation_id,
                "rap.status_code": msg.header.get("status_code", 0),
            },
        )
        msg.context.iter_span = iter_span
        span: Span = iter_span.__enter__()  # type: ignore
        inject(msg.header, context=context.get_current())
        return span

    async def process_request(self, request: Request) -> Request:
        if request.msg_type is constant.MT_MSG and not request.context.get_value("span", None):
            request.context.span = self._create_scope(request)
        elif request.msg_type is constant.MT_CHANNEL and not request.context.get_value("span", None):
            # A channel is a continuous activity that may involve the interaction of multiple coroutines
            request.context.span = self._create_scope(request)
        return await super().process_request(request)

    async def process_response(self, response_cb: ResponseCallable) -> Response:
        response: Response = await super().process_response(response_cb)
        if (response.msg_type is constant.MT_MSG) or (
            response.msg_type is constant.MT_CHANNEL
            and response.header.get("channel_life_cycle", "error") == constant.DECLARE
        ):
            span: Optional[Span] = response.context.get_value("span", None)
            if span:
                span.set_status(Status(status_code=StatusCode.OK))
                span.set_attribute("rap.status_code", response.status_code)
        return response

    async def on_context_exit(
        self, context_exit_cb: ContextExitCallable
    ) -> Tuple[ClientContext, Optional[Type[BaseException]], Optional[BaseException], Optional[TracebackType]]:
        _context, exc_type, exc_val, exc_tb = await super().on_context_exit(context_exit_cb)
        iter_span: Optional[Span] = _context.get_value("iter_span", None)
        if iter_span:
            iter_span.__exit__(exc_type, exc_val, exc_tb)
        return _context, exc_type, exc_val, exc_tb
