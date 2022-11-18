from typing import Optional

from opentelemetry import trace
from opentelemetry.context import attach, detach
from opentelemetry.context.context import Context
from opentelemetry.propagate import extract
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.trace.status import Status, StatusCode

from rap.common.msg import BaseMsgProtocol
from rap.common.utils import constant
from rap.server.model import Request, Response
from rap.server.plugin.processor.base import BaseProcessor, ResponseCallable


class OpenTelemetryProcessor(BaseProcessor):
    def __init__(
        self,
        tracer_provider: Optional[trace.TracerProvider] = None,
    ):
        self._tracer: trace.Tracer = trace.get_tracer(
            instrumenting_module_name="rap",
            tracer_provider=tracer_provider,
        )

    def _start_span(self, msg: BaseMsgProtocol, set_status_on_exception=False) -> trace.Span:
        service_name, group, func_name = msg.target.split("/")
        ctx: Context = extract(msg.header)
        msg.context.open_telemetry_token = attach(ctx)
        attributes: dict = {
            SpanAttributes.RPC_SYSTEM: "rap",
            SpanAttributes.RPC_GRPC_STATUS_CODE: 0,
            SpanAttributes.RPC_METHOD: func_name,
            SpanAttributes.RPC_SERVICE: service_name + "-" + group,
            "rap.msg_type": msg.msg_type,
            "rap.correlation_id": msg.correlation_id,
            "rap.status_code": msg.header.get("status_code", 0),
        }
        if "host" in msg.header:
            attributes[SpanAttributes.NET_PEER_NAME] = ":".join([str(i) for i in msg.header["host"]])
        return self._tracer.start_as_current_span(
            name=msg.target,
            kind=trace.SpanKind.SERVER,
            attributes=attributes,
            set_status_on_exception=set_status_on_exception,
        ).__enter__()  # type: ignore

    @staticmethod
    def _end_span(response: Response) -> None:
        response.context.span.end()
        detach(response.context.open_telemetry_token)

    async def process_request(self, request: Request) -> Request:
        if request.msg_type in (constant.MT_MSG, constant.MT_CHANNEL):
            self._start_span(request)
        return request

    async def process_response(self, response_cb: ResponseCallable) -> Response:
        response: Response = await super().process_response(response_cb)
        if response.msg_type is constant.MT_MSG:
            span: trace.Span = response.context.span
            span.set_status(Status(status_code=StatusCode.OK))
            span.set_attribute("rap.status_code", response.status_code)
            self._end_span(response)
        elif (
            response.msg_type is constant.MT_CHANNEL
            and response.header.get("channel_life_cycle", "error") == constant.DECLARE
        ):
            # The channel is created after receiving the request
            response.context.context_channel.add_done_callback(lambda f: self._end_span(response))
        return response

    # TODO end span
