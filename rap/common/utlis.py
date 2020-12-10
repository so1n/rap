import asyncio
import random
import string
import sys
import time

from dataclasses import dataclass
from typing import Dict

__all__ = [
    "get_event_loop",
    "response_num_dict",
    "Constant",
    "Event",
    "MISS_OBJECT",
    "gen_random_time_id",
    "gen_random_str_id",
    "parse_error",
]

from typing import Optional, Tuple

MISS_OBJECT = object()
_STR_LD = string.ascii_letters + string.digits


class Constant(object):
    VERSION: str = "0.5.3.1"
    USER_AGENT: str = "Python3"
    SOCKET_RECV_SIZE: int = 1024 ** 1

    DECLARE_REQUEST: int = 101
    DECLARE_RESPONSE: int = 201
    MSG_REQUEST: int = 102
    MSG_RESPONSE: int = 202
    DROP_REQUEST: int = 103
    DROP_RESPONSE: int = 203
    SERVER_EVENT: int = 301
    CLIENT_EVENT_RESPONSE: int = 401
    SERVER_ERROR_RESPONSE: int = 501

    EVENT_CLOSE_CONN: str = "event_close_conn"
    PING_EVENT: str = "ping"
    PONG_EVENT: str = "pong"


@dataclass()
class Event(object):
    event_name: str
    event_info: str

    def to_tuple(self) -> Tuple[str, str]:
        return self.event_name, self.event_info


def _get_event_loop():
    if sys.version_info >= (3, 7):
        return asyncio.get_running_loop

    return asyncio.get_event_loop


def gen_random_str_id(length: int = 8) -> str:
    return "".join(random.choice(_STR_LD) for i in range(length))


def gen_random_time_id(length: int = 8, time_length: int = 10) -> str:
    return str(int(time.time()))[-time_length:] + "".join(random.choice(_STR_LD) for i in range(length))


get_event_loop = _get_event_loop()


def parse_error(exception: Optional[Exception]) -> Optional[Tuple[str, str]]:
    error_response: Optional[Tuple[str, str]] = None
    if exception:
        error_response = (type(exception).__name__, str(exception))
    return error_response


response_num_dict: Dict[int, int] = {
    Constant.DECLARE_REQUEST: Constant.DECLARE_RESPONSE,
    Constant.MSG_REQUEST: Constant.MSG_RESPONSE,
    Constant.DROP_REQUEST: Constant.DROP_RESPONSE,
    Constant.CLIENT_EVENT_RESPONSE: -1,
}
