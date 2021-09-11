import asyncio
import inspect
from typing import Any, Callable, Coroutine, Dict, List, Optional, Union


def done_future(loop: Optional[asyncio.AbstractEventLoop] = None) -> asyncio.Future:
    future: asyncio.Future = asyncio.Future(loop=loop)
    future.set_result(True)
    return future


async def can_cancel_sleep(delay: float, *, loop: Optional[asyncio.AbstractEventLoop] = None) -> None:
    await asyncio.wait_for(asyncio.Future(), delay, loop=loop)


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

    for task in done:
        return task.result()


def del_future(future: asyncio.Future) -> None:
    """Cancel the running future and read the result"""
    if not future.cancelled():
        future.cancel()
    if future.done():
        future.result()


def safe_del_future(future: asyncio.Future) -> None:
    """Cancel the running future, read the result and not raise exc"""
    try:
        del_future(future)
    except Exception:
        pass


class Semaphore(asyncio.Semaphore):
    def __init__(self, value: int = 1, *, loop: asyncio.AbstractEventLoop = None):
        self.raw_value: int = value
        super(Semaphore, self).__init__(value, loop=loop)

    @property
    def inflight(self) -> int:
        value: int = self.raw_value - self._value  # type: ignore
        if value < 0:
            value = 0
        if value > self.raw_value:
            value = self.raw_value
        return value
