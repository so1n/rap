import socket
from typing import TYPE_CHECKING, Dict, List, Optional

from aio_statsd import StatsdClient  # type: ignore

from rap.common.utils import Constant
from rap.server.model import Request, Response, ServerEventEnum
from rap.server.plugin.processor.base import BaseProcessor

if TYPE_CHECKING:
    from rap.server.core import Server
    from rap.server.types import SERVER_EVENT_FN


class StatsdProcessor(BaseProcessor):
    """Note: not test..."""

    def __init__(self, statsd_client: StatsdClient, namespace: Optional[str] = None) -> None:
        self._statsd_client: StatsdClient = statsd_client

        self._namespace: str = namespace or f"rap.server.{socket.gethostname()}"

        self._channel_online_key: str = f"{self._namespace}.channel_online"
        self._channel_online_cnt: int = 0
        self._channel_key: str = f"{self._namespace}.channel"
        self._msg_key: str = f"{self._namespace}.msg"
        self._process_msg_key: str = f"{self._namespace}.msg.process"
        self._error_msg_key: str = f"{self._namespace}.msg.error"
        self._request_key: str = f"{self._namespace}.request"
        self._error_request_key: str = f"{self._namespace}.request.error"
        self.server_event_dict: Dict[ServerEventEnum, List["SERVER_EVENT_FN"]] = {
            ServerEventEnum.before_start: [self.start_event_handle]
        }

    def start_event_handle(self, app: "Server") -> None:
        def upload_metric(stats_dict: dict) -> None:
            for key, values in stats_dict.items():
                self._statsd_client.counter(f"{self._namespace}.key", values)
            self._statsd_client.counter(self._channel_online_key, self._channel_online_cnt)

        if self.app.window_state:
            self.app.window_state.add_callback(upload_metric)

    async def process_request(self, request: Request) -> Request:
        self._statsd_client.increment(self._request_key, 1)
        if request.num == Constant.MSG_REQUEST:
            self._statsd_client.increment(self._msg_key, 1)
            self._statsd_client.increment(self._process_msg_key, 1)
        host: str = request.header["host"]
        self._statsd_client.sets(f"{self._namespace}.online.{host}", 1)
        return request

    async def process_response(self, response: Response) -> Response:
        if response.num == Constant.MSG_RESPONSE:
            self._statsd_client.decrement(self._process_msg_key, 1)
            if response.header["status_code"] >= 400:
                # NOTE: Don't try to get the response body data
                self._statsd_client.increment(self._error_msg_key, 1)
        elif response.num == Constant.CHANNEL_RESPONSE:
            life_cycle: str = response.header.get("channel_life_cycle", "error")
            if life_cycle == Constant.DECLARE:
                self._channel_online_cnt += 1
                self._statsd_client.increment(self._channel_key, 1)
            elif life_cycle == Constant.DROP:
                self._channel_online_cnt -= 1
        elif response.num == Constant.SERVER_ERROR_RESPONSE:
            self._statsd_client.increment(self._error_request_key, 1)
        return response
