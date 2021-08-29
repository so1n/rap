from typing import TYPE_CHECKING, Dict, List, Optional

from rap.common.collect_statistics import Counter, Gauge
from rap.common.utils import Constant, EventEnum
from rap.server.model import Request, Response
from rap.server.plugin.processor.base import BaseProcessor

if TYPE_CHECKING:
    from rap.server.core import Server
    from rap.server.types import SERVER_EVENT_FN


class ClientPintProcessor(BaseProcessor):
    def __init__(self, diff: int = 10, prefix: str = "client_ping", expire: Optional[int] = None) -> None:
        self.expire: Optional[int] = expire
        self.request_online_counter: Counter = Counter(f"{prefix}_request_online")
        self.channel_online_cnt_counter: Counter = Counter(f"{prefix}_channel_online")
        self.error_cnt_gauge: Gauge = Gauge(f"{prefix}_error_cnt", diff=diff)
        self.server_event_dict: Dict[EventEnum, List["SERVER_EVENT_FN"]] = {
            EventEnum.before_start: [self.start_event_handle]
        }

    def start_event_handle(self, app: "Server") -> None:
        app.window_statistics.registry_metric(self.request_online_counter, expire=self.expire)
        app.window_statistics.registry_metric(self.channel_online_cnt_counter, expire=self.expire)
        app.window_statistics.registry_metric(self.error_cnt_gauge, expire=self.expire)

    async def process_request(self, request: Request) -> Request:
        if request.msg_type == Constant.MSG_REQUEST:
            self.request_online_counter.increment()
        return request

    async def process_response(self, response: Response) -> Response:
        if response.msg_type == Constant.MSG_RESPONSE:
            self.request_online_counter.decrement()
        elif response.msg_type == Constant.CHANNEL_RESPONSE:
            life_cycle: str = response.header.get("channel_life_cycle", "error")
            if life_cycle == Constant.DECLARE:
                self.channel_online_cnt_counter.increment()
            elif life_cycle == Constant.DROP:
                self.channel_online_cnt_counter.decrement()

        if response.status_code >= 400:
            self.error_cnt_gauge.increment()
        if response.target.endswith(Constant.PONG_EVENT):
            response.body.update(
                {
                    "request_online": self.request_online_counter.get_value(),
                    "channel_online": self.channel_online_cnt_counter.get_value(),
                    "error_cnt": self.error_cnt_gauge.get_value(),
                }
            )
        return response
