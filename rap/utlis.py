import asyncio
import sys
from enum import Enum


__all__ = ["get_event_loop", "Constant"]


class Constant(Enum):
    REQUEST: int = 0
    RESPONSE: int = 1
    SOCKET_RECV_SIZE: int = 1024 ** 2


def _get_event_loop():
    if sys.version_info >= (3, 7):
        return asyncio.get_running_loop

    return asyncio.get_event_loop


get_event_loop = _get_event_loop()
