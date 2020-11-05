import asyncio
import random
import string
import sys
import time

__all__ = ["get_event_loop", "Constant", "MISS_OBJECT", "gen_id", "parse_error"]

from typing import Optional, Tuple

MISS_OBJECT = object()
_STR_LD = string.ascii_letters + string.digits


class Constant(object):
    VERSION: str = "0.5.1"
    PROGRAMMING_LANGUAGE: str = "Python3"
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


def _get_event_loop():
    if sys.version_info >= (3, 7):
        return asyncio.get_running_loop

    return asyncio.get_event_loop


def gen_id(num: int = 8) -> str:
    return str(int(time.time() * 1000))[-10:] + "".join(random.choice(_STR_LD) for i in range(num))


get_event_loop = _get_event_loop()


def parse_error(exception: Optional[Exception]) -> Optional[Tuple[str, str]]:
    error_response: Optional[Tuple[str, str]] = None
    if exception:
        error_response = (type(exception).__name__, str(exception))
    return error_response
