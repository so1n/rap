import asyncio
import inspect
import random
import string
import sys
import time
from dataclasses import dataclass
from typing import Any, Callable, Coroutine, Dict, List, Sequence, Union

__all__ = [
    "Constant",
    "Event",
    "MISS_OBJECT",
    "RapFunc",
    "as_first_completed",
    "check_func_type",
    "gen_new_param_coro",
    "gen_random_time_id",
    "get_event_loop",
    "parse_error",
    "response_num_dict",
]

from typing import Optional, Tuple

from rap.common.types import is_type

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

    CHANNEL_TYPE: str = "channel"
    NORMAL_TYPE: str = "normal"

    DEFAULT_GROUP: str = "default"


class RapFunc(object):
    """
    Normally, a coroutine is created after calling the async function.
     In rap, hope that when the async function is called, it will still return the normal function,
     and the coroutine will not be generated until the await is called.
    """

    def __init__(self, func: Callable, raw_func: Callable):
        self.func: Callable = func
        self.raw_func: Callable = raw_func

        self._arg_param: Sequence[Any] = []
        self._kwargs_param: Dict[str, Any] = {}
        self._is_call: bool = False

        self.__name__ = self.func.__name__

    def _check(self) -> None:
        if not self._is_call:
            raise RuntimeError(f"{self.__class__.__name__} has not been called")
        self._is_call = False

    def __call__(self, *args: Any, **kwargs: Any) -> "RapFunc":
        self._arg_param = args
        self._kwargs_param = kwargs
        self._is_call = True
        return self

    def __await__(self) -> Any:
        """support await coro(x, x)"""
        self._check()
        return self.func(*self._arg_param, **self._kwargs_param).__await__()

    def __aiter__(self) -> Any:
        """support async for i in coro(x, x)"""
        self._check()
        return self.func(*self._arg_param, **self._kwargs_param).__aiter__()


@dataclass()
class Event(object):
    event_name: str
    event_info: str

    def to_tuple(self) -> Tuple[str, str]:
        return self.event_name, self.event_info


def _get_event_loop() -> Callable[[], asyncio.AbstractEventLoop]:
    if sys.version_info >= (3, 7):
        return asyncio.get_running_loop

    return asyncio.get_event_loop


get_event_loop = _get_event_loop()


def gen_random_time_id(length: int = 8, time_length: int = 10) -> str:
    return str(int(time.time()))[-time_length:] + "".join(random.choice(_STR_LD) for _ in range(length))


def parse_error(exception: Exception) -> Tuple[str, str]:
    return type(exception).__name__, str(exception)


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
    return func(**old_param_dict)


async def as_first_completed(
    future_list: List[Union[Coroutine, asyncio.Future]],
    not_cancel_future_list: Optional[List[Union[Coroutine, asyncio.Future]]] = None,
) -> Any:
    not_cancel_future_list = not_cancel_future_list if not_cancel_future_list else []
    future_list.extend(not_cancel_future_list)

    (done, pending) = await asyncio.wait(future_list, return_when=asyncio.FIRST_COMPLETED)
    for task in pending:
        if task not in not_cancel_future_list:
            task.cancel()

    result_list: List[Any] = [task.result() for task in done]
    if len(result_list) != 1:
        raise RuntimeError(f"{future_list} have {len(result_list)} result:{result_list}")
    return result_list[0]


response_num_dict: Dict[int, int] = {
    Constant.MSG_REQUEST: Constant.MSG_RESPONSE,
    Constant.CHANNEL_REQUEST: Constant.CHANNEL_RESPONSE,
    Constant.CLIENT_EVENT: -1,
}


def check_func_type(func: Callable, param_list: Sequence[Any], default_param_dict: Dict[str, Any]) -> None:
    func_sig: inspect.Signature = inspect.signature(func)
    for index, parameter_tuple in enumerate(func_sig.parameters.items()):
        name, parameter = parameter_tuple
        if parameter.default is parameter.empty:
            if not is_type(type(param_list[index]), parameter.annotation):
                raise TypeError(f"{param_list[index]} type must: {parameter.annotation}")
        else:
            if not is_type(type(default_param_dict.get(name, parameter.default)), parameter.annotation):
                raise TypeError(f"{default_param_dict[name]} type must: {parameter.annotation}")
