import asyncio
import inspect
import random
import string
import sys
import time
from dataclasses import dataclass
from typing import Any, Callable, Coroutine, Dict, List, Set, Union

__all__ = [
    "Constant",
    "Event",
    "MISS_OBJECT",
    "State",
    "as_first_completed",
    "gen_new_param_coro",
    "gen_random_time_id",
    "gen_random_str_id",
    "get_event_loop",
    "parse_error",
    "response_num_dict",
]

from typing import Optional, Tuple

MISS_OBJECT = object()
_STR_LD = string.ascii_letters + string.digits


class Constant(object):
    VERSION: str = "0.1"  # protocol version
    USER_AGENT: str = "Python3-0.5.3"
    SOCKET_RECV_SIZE: int = 1024 ** 1

    SERVER_ERROR_RESPONSE: int = 100
    MSG_REQUEST: int = 101
    MSG_RESPONSE: int = 201
    CHANNEL_REQUEST: int = 102
    CHANNEL_RESPONSE: int = 202
    CLIENT_EVENT: int = 103
    SERVER_EVENT: int = 203

    EVENT_CLOSE_CONN: str = "event_close_conn"
    PING_EVENT: str = "ping"
    PONG_EVENT: str = "pong"

    DECLARE: str = "declare"
    MSG: str = "MSG"
    DROP: str = "drop"


class State(object):
    """copy from starlette"""

    def __init__(self, state: Optional[Dict] = None):
        if state is None:
            state = {}
        super(State, self).__setattr__("_state", state)

    def __setattr__(self, key: Any, value: Any) -> None:
        self._state[key] = value

    def __getattr__(self, key: Any) -> Any:
        try:
            return self._state[key]
        except KeyError:
            raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{key}'")

    def __len__(self):
        return len(self._state)

    def __delattr__(self, key: Any) -> None:
        del self._state[key]


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


get_event_loop = _get_event_loop()


def gen_random_str_id(length: int = 8) -> str:
    return "".join(random.choice(_STR_LD) for _ in range(length))


def gen_random_time_id(length: int = 8, time_length: int = 10) -> str:
    return str(int(time.time()))[-time_length:] + "".join(random.choice(_STR_LD) for _ in range(length))


def parse_error(exception: Optional[Exception]) -> Optional[Tuple[str, str]]:
    error_response: Optional[Tuple[str, str]] = None
    if exception:
        error_response = (type(exception).__name__, str(exception))
    return error_response


def gen_new_param_coro(coro: Coroutine, new_param_dict: Dict[str, Any]) -> Coroutine:
    """
    >>> async def demo(a: int, b: int) -> int:
    ...     return a + b
    >>> value1: int = asyncio.run(demo(1, 3))
    >>> value2: int = asyncio.run(gen_new_param_coro(demo(1, 5), {"b": 3}))
    >>> assert value1 == value2
    """
    if not asyncio.iscoroutine(coro):
        raise TypeError("")
    qualname: str = coro.__qualname__.split(".<locals>", 1)[0].rsplit(".", 1)[0]
    func: Callable = getattr(inspect.getmodule(coro.cr_frame), qualname)
    old_param_dict: Dict[str, Any] = coro.cr_frame.f_locals
    for key, value in new_param_dict.items():
        if key not in old_param_dict:
            raise KeyError(f"Not found {key} in {old_param_dict.keys()}")
        old_param_dict[key] = value
    return func(old_param_dict)


async def as_first_completed(future_list: List[Union[Coroutine, asyncio.Future]], *, timeout=None):
    if asyncio.isfuture(future_list) or asyncio.iscoroutine(future_list):
        raise TypeError(f"expect a list of futures, not {type(future_list).__name__}")
    loop: asyncio.AbstractEventLoop = get_event_loop()
    todo_future_set: Set[asyncio.Future] = {asyncio.ensure_future(f, loop=loop) for f in set(future_list)}
    done_queue: asyncio.Queue = asyncio.Queue(loop=loop)
    timeout_handle = None

    def _on_timeout():
        for f in todo_future_set:
            f.remove_done_callback(_on_completion)
            done_queue.put_nowait(None)  # Queue a dummy value for _wait_for_one().
        todo_future_set.clear()  # Can't do todo.remove(f) in the loop.

    def _on_completion(f):
        if not todo_future_set:
            return  # _on_timeout() was here first.
        todo_future_set.remove(f)
        done_queue.put_nowait(f)
        if not todo_future_set and timeout_handle is not None:
            timeout_handle.cancel()

    async def _wait_for_one():
        f = await done_queue.get()
        if f is None:
            # Dummy value from _on_timeout().
            raise asyncio.TimeoutError
        return f.result()  # May raise f.exception().

    for f in todo_future_set:
        f.add_done_callback(_on_completion)
    if todo_future_set and timeout is not None:
        timeout_handle = loop.call_later(timeout, _on_timeout)

    try:
        return await _wait_for_one()
    finally:
        for f in todo_future_set:
            if f.cancelled():
                f.cancel()
            if timeout_handle and timeout_handle.cancelled():
                timeout_handle.cancel()


response_num_dict: Dict[int, int] = {
    Constant.MSG_REQUEST: Constant.MSG_RESPONSE,
    Constant.CHANNEL_REQUEST: Constant.CHANNEL_RESPONSE,
    Constant.CLIENT_EVENT: -1,
}
