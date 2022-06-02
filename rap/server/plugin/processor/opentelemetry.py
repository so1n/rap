from typing import Optional

from opentelemetry import trace
from opentelemetry.context import attach, detach
from opentelemetry.context.context import Context
from opentelemetry.propagate import extract
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.trace.status import Status, StatusCode

from rap.common.utils import constant
from rap.server.model import Request, Response
from rap.server.plugin.processor.base import BaseProcessor


class OpenTelemetryProcessor(BaseProcessor):
    def __init__(
        self,
        tracer_provider: Optional[trace.TracerProvider] = None,
    ):
        self._tracer: trace.Tracer = trace.get_tracer(
            instrumenting_module_name="rap",
            tracer_provider=tracer_provider,
        )

    def _start_span(self, request: Request, set_status_on_exception=False) -> trace.Span:
        service_name, group, func_name = request.target.split("/")
        ctx: Context = extract(request.header)
        request.context.open_telemetry_token = attach(ctx)
        attributes: dict = {
            SpanAttributes.RPC_SYSTEM: "rap",
            SpanAttributes.RPC_GRPC_STATUS_CODE: 0,
            SpanAttributes.RPC_METHOD: func_name,
            SpanAttributes.RPC_SERVICE: service_name + "-" + group,
            "rap.msg_type": request.msg_type,
            "rap.correlation_id": request.correlation_id,
            "rap.status_code": request.header.get("status_code", 0),
        }
        if "host" in request.header:
            attributes[SpanAttributes.NET_PEER_NAME] = ":".join([str(i) for i in request.header["host"]])
        return self._tracer.start_as_current_span(
            name=request.target,
            kind=trace.SpanKind.SERVER,
            attributes=attributes,
            set_status_on_exception=set_status_on_exception,
        ).__enter__()  # type: ignore

    @staticmethod
    def _end_span(response: Response) -> None:
        response.context.span.end()
        detach(response.context.open_telemetry_token)

    async def process_request(self, request: Request) -> Request:
        if request.msg_type is constant.MSG_REQUEST and not request.context.get_value("span", None):
            request.context.span = self._start_span(request)
        elif request.msg_type is constant.CHANNEL_REQUEST and not request.context.get_value("span", None):
            # A channel is a continuous activity that may involve the interaction of multiple coroutines
            request.context.span = self._start_span(request)
        return request

    async def process_response(self, response: Response) -> Response:
        if response.msg_type is constant.MSG_RESPONSE:
            span: trace.Span = response.context.span
            span.set_status(Status(status_code=StatusCode.OK))
            span.set_attribute("rap.status_code", response.status_code)
            self._end_span(response)
        elif (
            response.msg_type is constant.CHANNEL_RESPONSE
            and response.header.get("channel_life_cycle", "error") == constant.DECLARE
        ):
            # The channel is created after receiving the request
            response.context.context_channel.add_done_callback(lambda f: self._end_span(response))
        return response

    async def process_exc(self, response: Response) -> Response:
        span: Optional[trace.Span] = response.context.get_value("span", None)
        if span and response.msg_type is constant.MSG_RESPONSE:
            span.set_attribute("rap.status_code", response.status_code)
            if response.exc:
                span.record_exception(response.exc)
                span.set_status(
                    Status(status_code=StatusCode.ERROR, description=f"{type(response.exc).__name__}: {response.exc}")
                )
                self._end_span(response)
        return response
