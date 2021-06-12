import random
from typing import Any, Dict, Tuple

from rap.client.model import Request, Response
from rap.client.processor.base import BaseProcessor
from rap.common.exceptions import ServerError
from rap.common.state import WindowState
from rap.common.utils import Constant


class BaseCircuitBreakerProcessor(BaseProcessor):
    exc: Exception = NotImplementedError()

    def __init__(
        self,
        interval: int = 2 * 60,
        fuse_k: float = 2.0,  # google sre default
        fuse_enable_cnt: int = 100,
    ):
        self._prefix: str = "fuse"
        self._fuse_enable_cnt: int = fuse_enable_cnt
        self._fuse_window_state: WindowState = WindowState(interval=interval)
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
                self._probability_dict[index] = max(0.0, (total - fuse_k * (total - error_cnt)) / (total + 1))

        self._fuse_window_state.add_priority_callback(upload_probability)

    def start_event_handle(self) -> None:
        if self._fuse_window_state.is_closed:
            self._fuse_window_state.change_state()

    def stop_event_handle(self) -> None:
        if not self._fuse_window_state.is_closed:
            self._fuse_window_state.close()

    def get_request_index(self, request: Request) -> str:
        raise NotImplementedError

    def get_response_index(self, response: Response) -> str:
        raise NotImplementedError

    async def process_request(self, request: Request) -> Request:
        if request.num == Constant.CLIENT_EVENT:
            # do not process event
            return request
        index: str = self.get_request_index(request)
        total: int = self._fuse_window_state.get_value((self._prefix, index, "total"), 0)  # type: ignore
        if total > self._fuse_enable_cnt:
            if random.randint(0, 100) > self._probability_dict.get(index, 0.0) * 100:
                raise self.exc

        self._fuse_window_state.increment((self._prefix, index, "total"))
        return request

    async def process_exc(self, response: Response, exc: Exception) -> Tuple[Response, Exception]:
        self._fuse_window_state.increment((self._prefix, self.get_response_index(response), "error"))
        return response, exc


class HostCircuitBreakerProcessor(BaseCircuitBreakerProcessor):
    exc: Exception = ServerError("Service Unavailable")

    def get_request_index(self, request: Request) -> str:
        return request.header["host"]

    def get_response_index(self, response: Response) -> str:
        return response.conn.peer_tuple[0]


class FuncCircuitBreakerProcessor(BaseCircuitBreakerProcessor):
    exc: Exception = ServerError("Service's func Unavailable")

    def get_request_index(self, request: Request) -> str:
        return request.func_name

    def get_response_index(self, response: Response) -> str:
        return response.func_name
