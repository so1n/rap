from typing import Optional

from opentelemetry import context, trace
from opentelemetry.propagate import inject
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.trace.status import Status, StatusCode

from rap.client.model import Request, Response
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

    def _start_span(self, request: Request) -> trace.Span:
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
        return span

    @staticmethod
    def _end_span(response: Response) -> None:
        response.context.iter_span.__exit__(None, None, None)
        response.context.span.end()

    async def process_request(self, request: Request) -> Request:
        if request.msg_type is constant.MSG_REQUEST and not request.context.get_value("span", None):
            request.context.span = self._start_span(request)
        elif request.msg_type is constant.CHANNEL_REQUEST and not request.context.get_value("span", None):
            # A channel is a continuous activity that may involve the interaction of multiple coroutines
            request.context.span = self._start_span(request)
            print(request)
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
            print(response)
            response.context.user_channel.add_done_callback(lambda f: self._end_span(response))
        return response

    async def process_exc(self, response: Response) -> Response:
        span: Optional[trace.Span] = response.context.get_value("span", None)
        if span and response.msg_type is constant.MSG_RESPONSE:
            span.set_attribute("rap.status_code", response.status_code)
            if response.exc:
                # span.record_exception(response.exc)
                if response.tb:
                    stacktrace = response.tb
                else:
                    # workaround for python 3.4, format_exc can raise
                    # an AttributeError if the __context__ on
                    # an exception is None
                    stacktrace = "Exception occurred on stacktrace formatting"
                _attributes = {
                    "exception.type": response.exc.__class__.__name__,
                    "exception.message": str(response.exc),
                    "exception.stacktrace": stacktrace,
                    "exception.escaped": str(False),
                }
                span.add_event(name="exception", attributes=_attributes)
                span.set_status(
                    Status(status_code=StatusCode.ERROR, description=f"{type(response.exc).__name__}: {response.exc}")
                )
            self._end_span(response)
        return response
