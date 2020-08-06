import asyncio
import sys

__all__ = ["get_event_loop", "Constant"]


class Constant(object):
    REQUEST: int = 0
    RESPONSE: int = 1
    SOCKET_RECV_SIZE: int = 1024 ** 1


def _get_event_loop():
    if sys.version_info >= (3, 7):
        return asyncio.get_running_loop

    return asyncio.get_event_loop


get_event_loop = _get_event_loop()
