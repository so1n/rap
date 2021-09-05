import asyncio
import inspect
import random
import string
import sys
import time
from enum import Enum, auto
from typing import Any, Callable, Coroutine, Dict, List, Optional, Sequence, Tuple, Union

from rap.common.types import is_type

__all__ = [
    "Constant",
    "EventEnum",
    "RapFunc",
    "as_first_completed",
    "check_func_type",
    "del_future",
    "gen_new_param_coro",
    "gen_random_time_id",
    "get_event_loop",
    "parse_error",
    "param_handle",
    "response_num_dict",
]


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


def _get_event_loop() -> Callable[[], asyncio.AbstractEventLoop]:
    """get event loop in runtime"""
    if sys.version_info >= (3, 7):
        return asyncio.get_running_loop

    return asyncio.get_event_loop


get_event_loop = _get_event_loop()


def gen_random_time_id(length: int = 8, time_length: int = 10) -> str:
    """Simply generate ordered id"""
    return str(int(time.time()))[-time_length:] + "".join(random.choice(_STR_LD) for _ in range(length))


def parse_error(exception: Exception) -> Tuple[str, str]:
    """parse python exc and return exc name and info"""
    return type(exception).__name__, str(exception)


def gen_new_param_coro(coro: Coroutine, new_param_dict: Dict[str, Any]) -> Coroutine:
    """
    Return a new coro according to the parameters
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
    """Wait for multiple coroutines to process data, until one of the coroutines returns data,
    and the remaining coroutines are cancelled
    """
    not_cancel_future_list = not_cancel_future_list if not_cancel_future_list else []
    future_list.extend(not_cancel_future_list)

    (done, pending) = await asyncio.wait(future_list, return_when=asyncio.FIRST_COMPLETED)
    for task in pending:
        if task not in not_cancel_future_list:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

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
    """Check whether the input parameter type is consistent with the function parameter type"""
    func_sig: inspect.Signature = inspect.signature(func)
    for index, parameter_tuple in enumerate(func_sig.parameters.items()):
        name, parameter = parameter_tuple
        if parameter.default is parameter.empty:
            if not is_type(type(param_list[index]), parameter.annotation):
                raise TypeError(f"{param_list[index]} type must: {parameter.annotation}")
        else:
            if not is_type(type(default_param_dict.get(name, parameter.default)), parameter.annotation):
                raise TypeError(f"{default_param_dict[name]} type must: {parameter.annotation}")


def param_handle(func: Callable, param_list: Sequence[Any], default_param_dict: Dict[str, Any]) -> Tuple[Any, ...]:
    """Check whether the parameter is legal and whether the parameter type is correct"""
    new_param_list: Tuple[Any, ...] = inspect.signature(func).bind(*param_list, **default_param_dict).args
    check_func_type(func, param_list, default_param_dict)
    return new_param_list


def del_future(future: asyncio.Future) -> None:
    """Cancel the running future and read the result"""
    if not future.cancelled():
        future.cancel()
    if future.done():
        future.result()


class Semaphore(asyncio.Semaphore):
    def __init__(self, value: int = 1, *, loop: asyncio.AbstractEventLoop = None):
        self.raw_value: int = value
        super(Semaphore, self).__init__(value, loop=loop)

    @property
    def value(self) -> int:
        return self._value  # type: ignore


class EventEnum(Enum):
    before_start = auto()
    after_start = auto()
    before_end = auto()
    after_end = auto()
