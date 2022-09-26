import logging
import random
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from rap.client.model import Request, Response
from rap.client.processor.base import BaseProcessor
from rap.client.types import CLIENT_EVENT_FN
from rap.common.collect_statistics import WindowStatistics
from rap.common.utils import EventEnum, constant

if TYPE_CHECKING:
    from rap.client.core import BaseClient

logger: logging.Logger = logging.getLogger(__name__)


class CircuitBreakerExc(Exception):
    pass


class BaseCircuitBreakerProcessor(BaseProcessor):
    """The simplest circuit breaker based on the idea of google sre document"""

    exc: Exception = NotImplementedError()
    _window_statistics: WindowStatistics

    def __init__(
        self,
        k: float = 2.0,
        expire: int = 180,
        interval: int = 120,
        prefix: str = "circuit_breaker",
        window_statistics: Optional[WindowStatistics] = None,
    ):
        """
        :param interval: sliding window interval
        :param k: google ste circuit breaker default k
        :param expire: metric expire time
        :param interval: metric data change interval
        :param prefix: metric key prefix
        """
        self._k: float = k
        self._prefix: str = prefix
        self._expire: int = expire
        self._interval: int = interval
        if window_statistics:
            self._window_statistics = window_statistics

        self._probability_dict: Dict[str, float] = {}

        self.event_dict: Dict[EventEnum, List[CLIENT_EVENT_FN]] = {
            EventEnum.after_start: [self.start_event_handle],
            EventEnum.before_end: [self.stop_event_handle],
        }

    def start_event_handle(self, app: "BaseClient") -> None:
        if not getattr(self, "_window_statistics", None):
            self._window_statistics = app.window_statistics
        if self._window_statistics.max_internal < self._interval:
            logger.warning(f"Ws:{self._window_statistics.__class__.__name__} max_internal < 120, must use new ws")
            self._window_statistics = WindowStatistics(interval=1, max_interval=self._interval, statistics_interval=1)

        def upload_probability(stats_dict: Dict[Any, int]) -> None:
            _dict: Dict[str, Dict[str, int]] = {}
            for key, value in stats_dict.items():
                if not key.startswith("counter_" + self._prefix):
                    continue
                _, index, type_ = key.split("|")
                if index not in _dict:
                    _dict[index] = {}
                _dict[index][type_] = value
            for index, metric_dict in _dict.items():
                total: int = metric_dict.get("total", 0)
                error_cnt: int = metric_dict.get("error", 0)
                self._probability_dict[index] = max(0.0, (total - self._k * (total - error_cnt)) / (total + 1))

        self._window_statistics.add_priority_callback(upload_probability)
        if self._window_statistics.is_closed:
            self._window_statistics.statistics_data()

    def stop_event_handle(self, app: "BaseClient") -> None:
        if not self._window_statistics.is_closed:
            self._window_statistics.close()

    def get_index_from_request(self, request: Request) -> str:
        raise NotImplementedError

    def get_index_from_response(self, response: Response) -> str:
        raise NotImplementedError

    async def process_request(self, request: Request) -> Request:
        if request.msg_type in (constant.CLIENT_EVENT, constant.SERVER_EVENT):
            # do not process event
            return request
        index: str = self.get_index_from_request(request)
        total_key: str = f"{self._prefix}|{index}|total"
        self._window_statistics.set_counter_value(total_key, expire=self._expire, diff=self._interval)
        if random.randint(0, 100) < self._probability_dict.get(index, 0.0) * 100:
            error_key: str = f"{self._prefix}|{index}|error"
            self._window_statistics.set_counter_value(error_key, expire=self._expire, diff=self._interval)
            raise self.exc

        return request

    async def process_exc(self, response: Response) -> Response:
        error_key: str = f"{self._prefix}|{self.get_index_from_response(response)}|error"
        self._window_statistics.set_counter_value(error_key, expire=self._expire, diff=self._interval)
        return response


class HostCircuitBreakerProcessor(BaseCircuitBreakerProcessor):
    exc: Exception = CircuitBreakerExc("Service Unavailable")

    def get_index_from_request(self, request: Request) -> str:
        return request.context.server_info["host"][0]

    def get_index_from_response(self, response: Response) -> str:
        return response.context.server_info["host"][0]


class FuncCircuitBreakerProcessor(BaseCircuitBreakerProcessor):
    exc: Exception = CircuitBreakerExc("Service's func Unavailable")

    def get_index_from_request(self, request: Request) -> str:
        return request.target

    def get_index_from_response(self, response: Response) -> str:
        return response.target
