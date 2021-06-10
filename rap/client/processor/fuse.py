import random
from typing import Dict, Tuple

from rap.client.model import Request, Response
from rap.client.processor.base import BaseProcessor
from rap.common.exceptions import ServerError
from rap.common.state import WindowState


class FuseProcessor(BaseProcessor):
    def __init__(
        self,
        interval: int = 2 * 60,
        fuse_k: float = 2.0,  # google sre default
        fuse_enable_cnt: int = 100,
    ):
        self._fuse_enable_cnt: int = fuse_enable_cnt
        self._fuse_window_state: WindowState = WindowState(interval=interval)
        self._probability_dict: Dict[str, float] = {}

        def upload_probability(stats_dict: Dict[str, int]) -> None:
            _dict: Dict[str, Dict[str, int]] = {}
            for key, value in stats_dict.items():
                index = key.rfind(":")
                host: str = key[:index]
                real_key: str = key[index + 1 :]
                if host not in _dict:
                    _dict[host] = {}
                _dict[host][real_key] = value
            for host, metric_dict in _dict.items():
                total: int = metric_dict["total"]
                error_cnt: int = metric_dict["error"]
                self._probability_dict[host] = max(0.0, (total - fuse_k * (total - error_cnt)) / (total + 1))

        self._fuse_window_state.add_priority_callback(upload_probability)

    def start_event_handle(self) -> None:
        if self._fuse_window_state.is_closed:
            self._fuse_window_state.change_state()

    def stop_event_handle(self) -> None:
        if not self._fuse_window_state.is_closed:
            self._fuse_window_state.close()

    async def process_request(self, request: Request) -> Request:
        host: str = request.header["host"]
        total: int = self._fuse_window_state.get_value(f"{host}:total", 0)  # type: ignore
        if total > self._fuse_enable_cnt:
            if random.randint(0, 100) > self._probability_dict.get(host, 0.0) * 100:
                raise ServerError("Service Unavailable")

        self._fuse_window_state.increment(f"{request.header['host']}:total")
        return request

    async def process_exc(self, response: Response, exc: Exception) -> Tuple[Response, Exception]:
        self._fuse_window_state.increment(f"{response.header['host']}:error")
        return response, exc
