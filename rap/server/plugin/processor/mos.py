from typing import TYPE_CHECKING, Dict, List

import psutil

from rap.common.asyncio_helper import get_event_loop
from rap.common.collect_statistics import Counter, Gauge
from rap.common.utils import EventEnum, constant
from rap.server.model import Request, Response
from rap.server.plugin.processor.base import BaseProcessor

if TYPE_CHECKING:
    from rap.server.core import Server
    from rap.server.types import SERVER_EVENT_FN


psutil.cpu_percent()


class MosProcessor(BaseProcessor):
    def __init__(
        self,
        diff: int = 10,
        prefix: str = "mos",
        max_request_online: int = 100,
        max_channel_online: int = 100,
        max_error_cnt: int = 100,
        max_request_cnt: int = 1000,
        max_response_cnt: int = 1000,
    ) -> None:
        """
        Provide the server's mos through the client's ping-pong
        :param diff: how many windows are a time period
        :param prefix: metric name prefix
        :param max_request_online: The server accepts the maximum number of concurrent
        :param max_channel_online: The maximum number of channels accepted by the server
        :param max_error_cnt: The maximum number of errors accepted within the server time window
        :param max_request_cnt: The maximum number of requests accepted within the server time window
        :param max_response_cnt: The maximum number of responses accepted within the server time window
        """
        self.request_online_counter: Counter = Counter(f"{prefix}_request_online")
        self.channel_online_cnt_counter: Counter = Counter(f"{prefix}_channel_online")
        self.request_cnt_gauge: Gauge = Gauge(f"{prefix}_request_cnt", diff=diff)
        self.response_cnt_gauge: Gauge = Gauge(f"{prefix}_response_cnt", diff=diff)
        self.error_cnt_gauge: Gauge = Gauge(f"{prefix}_error_cnt", diff=diff)
        self.server_event_dict: Dict[EventEnum, List["SERVER_EVENT_FN"]] = {
            EventEnum.before_start: [self.start_event_handle],
            EventEnum.after_end: [self.stop_event_handle],
        }
        self._max_request_online: int = max_request_online
        self._max_channel_online: int = max_channel_online
        self._max_error_cnt: int = max_error_cnt
        self._max_request_cnt: int = max_request_cnt
        self._max_response_cnt: int = max_response_cnt
        self._run_cpu_percent: bool = False
        self._cpu_percent: float = psutil.cpu_percent()

    def _get_cpu_percent(self) -> None:
        self._cpu_percent = psutil.cpu_percent()
        if self._run_cpu_percent:
            get_event_loop().call_later(1, self._get_cpu_percent)

    def start_event_handle(self, app: "Server") -> None:
        self._run_cpu_percent = True
        self._get_cpu_percent()
        app.window_statistics.registry_metric(self.request_online_counter)
        app.window_statistics.registry_metric(self.channel_online_cnt_counter)
        app.window_statistics.registry_metric(self.request_cnt_gauge)
        app.window_statistics.registry_metric(self.response_cnt_gauge)
        app.window_statistics.registry_metric(self.error_cnt_gauge)

    def stop_event_handle(self, app: "Server") -> None:
        self._run_cpu_percent = False

    async def process_request(self, request: Request) -> Request:
        self.request_cnt_gauge.increment()
        if request.msg_type == constant.MSG_REQUEST:
            self.request_online_counter.increment()
        return request

    async def process_response(self, response: Response) -> Response:
        self.response_cnt_gauge.increment()
        if response.msg_type == constant.MSG_RESPONSE:
            self.request_online_counter.decrement()
        elif response.msg_type == constant.CHANNEL_RESPONSE:
            life_cycle: str = response.header.get("channel_life_cycle", "error")
            if life_cycle == constant.DECLARE:
                self.channel_online_cnt_counter.increment()
            elif life_cycle == constant.DROP:
                self.channel_online_cnt_counter.decrement()

        if response.status_code >= 400:
            self.error_cnt_gauge.increment()
        if response.target.endswith(constant.PONG_EVENT):
            mos: int = int(
                5
                * (1 - self._cpu_percent)
                * (1 - (self.request_cnt_gauge.get_statistics_value() / self._max_request_cnt))
                * (1 - (self.response_cnt_gauge.get_statistics_value() / self._max_response_cnt))
                * (1 - (self.error_cnt_gauge.get_statistics_value() - self._max_error_cnt))
                * (1 - (self.request_online_counter.get_statistics_value() - self._max_request_online))
                * (1 - (self.channel_online_cnt_counter.get_statistics_value() - self._max_channel_online))
            )
            if mos < 0:
                mos = 0
            if mos > 10:
                mos = 5
            response.body["mos"] = mos
        return response
