import socket
import time
from typing import TYPE_CHECKING, Dict, List

from prometheus_client import Counter, Gauge, Histogram, start_http_server  # type: ignore

from rap.common.utils import EventEnum, constant
from rap.server.model import Request, Response
from rap.server.plugin.processor.base import BaseProcessor, ContextExitCallable, ContextExitType, ResponseCallable

if TYPE_CHECKING:
    from rap.server.core import Server
    from rap.server.types import SERVER_EVENT_FN


label_list: List[str] = ["service", "host_name", "target", "group"]

# normal request
msg_request_count: "Counter" = Counter("msg_request_total", "Count of msg requests", label_list)
msg_response_count: "Counter" = Counter("msg_response_total", "Count of msg response", label_list + ["status_code"])
msg_request_time: "Histogram" = Histogram("msg_request_time", "Histogram of msg request time by target", label_list)
msg_request_in_progress: "Gauge" = Gauge("msg_request_in_progress", "Gauge of current msg request", label_list)
# channel
channel_count: "Counter" = Counter("channel_request_total", "Count of channel request", label_list)
channel_in_progress: "Gauge" = Gauge("channel_in_progress", "Gauge of current channel request", label_list)
# total
request_count: "Counter" = Counter("request_total", "Count of requests", label_list + ["msg_type"])
response_count: "Counter" = Counter("response_total", "Count of response", label_list + ["msg_type", "status_code"])
exc_count: "Counter" = Counter("exc_total", "Count of exc", label_list + ["msg_type"])


class PrometheusProcessor(BaseProcessor):
    def __init__(self, service_name: str, prometheus_host: str, prometheus_port: int):
        self._service_name: str = service_name
        self._prometheus_host: str = prometheus_host
        self._prometheus_port: int = prometheus_port
        self.host_name: str = socket.gethostname()
        self.server_event_dict: Dict[EventEnum, List["SERVER_EVENT_FN"]] = {
            EventEnum.before_start: [self.start_event_handle]
        }

    def start_event_handle(self, app: "Server") -> None:
        start_http_server(self._prometheus_host, self._prometheus_port)

    async def process_request(self, request: Request) -> Request:
        label_list: list = [self._service_name, self.host_name, request.target]
        request_count.labels(*label_list, request.msg_type).inc()
        if request.msg_type == constant.MSG_REQUEST:
            request.context.start_time = time.time()
            msg_request_count.labels(*label_list).inc()
            msg_request_in_progress.labels(*label_list).inc()
        return request

    async def process_response(self, response_cb: ResponseCallable) -> Response:
        resp = None
        try:
            response: Response = await super().process_response(response_cb)
            resp = response
            return response
        except Exception as e:
            resp, _ = await response_cb(False)
            raise e
        finally:
            response = resp  # type: ignore
            label_list: list = [self._service_name, self.host_name, response.target]
            response_count.labels(*label_list, response.msg_type, response.status_code).inc()
            if response.msg_type == constant.MSG_RESPONSE:
                msg_response_count.labels(*label_list, response.status_code).inc()
                msg_request_in_progress.labels(*label_list).dec()
                msg_request_time.labels(*label_list).observe(time.time() - response.context.start_time)
            elif response.msg_type == constant.CHANNEL_RESPONSE:
                life_cycle: str = response.header.get("channel_life_cycle", "error")
                if life_cycle == constant.DECLARE:
                    channel_count.labels(*label_list).inc()
                    channel_in_progress.labels(*label_list).inc()
                elif life_cycle == constant.DROP:
                    channel_in_progress.labels(*label_list).dec()

    async def on_context_exit(self, context_exit_cb: ContextExitCallable) -> ContextExitType:
        context, exc_type, exc_val, exc_tb = await super().on_context_exit(context_exit_cb)
        if exc_type:
            # TODO
            # exc_count.labels(self._service_name, self.host_name, context.target, context.msg_type).inc()
            pass
        return context, exc_type, exc_val, exc_tb
