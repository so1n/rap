import random
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Tuple

from rap.client.model import Request, Response
from rap.client.processor.base import BaseProcessor
from rap.client.types import CLIENT_EVENT_FN
from rap.common.exceptions import ServerError
from rap.common.state import WindowState
from rap.common.utils import Constant, EventEnum

if TYPE_CHECKING:
    from rap.client.core import BaseClient


class BaseCircuitBreakerProcessor(BaseProcessor):
    """The simplest circuit breaker based on the idea of google sre document"""

    exc: Exception = NotImplementedError()

    def __init__(
        self,
        interval: int = 2 * 60,
        k: float = 2.0,  # google sre default
        enable_cnt: int = 100,
    ):
        """
        interval: sliding window interval
        k: google ste circuit breaker default k
        enable_cnt: Set to enable when the total number of requests is greater than a certain value
        """
        self._prefix: str = "circuit_breaker"
        self._enable_cnt: int = enable_cnt
        self._window_state: WindowState = WindowState(interval=interval)
        self._probability_dict: Dict[str, float] = {}

        def upload_probability(stats_dict: Dict[Any, int]) -> None:
            _dict: Dict[str, Dict[str, int]] = {}
            for key, value in stats_dict.items():
                if isinstance(key, tuple) and len(key) == 3:
                    prefix, index, type_ = key
                    if prefix != self._prefix:
                        continue
                    if index not in _dict:
                        _dict[index] = {}
                    _dict[index][type_] = value
            for index, metric_dict in _dict.items():
                total: int = metric_dict["total"]
                error_cnt: int = metric_dict["error"]
                self._probability_dict[index] = max(0.0, (total - k * (total - error_cnt)) / (total + 1))

        self._window_state.add_priority_callback(upload_probability)
        self.event_dict: Dict[EventEnum, List[CLIENT_EVENT_FN]] = {
            EventEnum.after_start: [self.start_event_handle],
            EventEnum.before_end: [self.stop_event_handle],
        }

    def start_event_handle(self, app: "BaseClient") -> None:
        if self._window_state.is_closed:
            self._window_state.change_state(app)

    def stop_event_handle(self, app: "BaseClient") -> None:
        if not self._window_state.is_closed:
            self._window_state.close()

    def get_request_index(self, request: Request) -> str:
        raise NotImplementedError

    def get_response_index(self, response: Response) -> str:
        raise NotImplementedError

    async def process_request(self, request: Request) -> Request:
        if request.msg_type == Constant.CLIENT_EVENT:
            # do not process event
            return request
        index: str = self.get_request_index(request)
        total: int = self._window_state.get_value((self._prefix, index, "total"), 0)  # type: ignore
        if total > self._enable_cnt:
            if random.randint(0, 100) < self._probability_dict.get(index, 0.0) * 100:
                raise self.exc

        self._window_state.increment((self._prefix, index, "total"))
        return request

    async def process_exc(self, response: Response, exc: Exception) -> Tuple[Response, Exception]:
        self._window_state.increment((self._prefix, self.get_response_index(response), "error"))
        return response, exc


class HostCircuitBreakerProcessor(BaseCircuitBreakerProcessor):
    exc: Exception = ServerError("Service Unavailable")

    def get_request_index(self, request: Request) -> str:
        return request.header["host"][0]

    def get_response_index(self, response: Response) -> str:
        return response.conn.peer_tuple[0]


class FuncCircuitBreakerProcessor(BaseCircuitBreakerProcessor):
    exc: Exception = ServerError("Service's func Unavailable")

    def get_request_index(self, request: Request) -> str:
        return request.target

    def get_response_index(self, response: Response) -> str:
        return response.target
