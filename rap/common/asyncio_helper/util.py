import asyncio
import sys
from typing import Any, Coroutine, List, Optional, Union


def current_task(loop: Optional[asyncio.AbstractEventLoop] = None) -> "Optional[asyncio.Task[Any]]":
    """return current task"""
    if sys.version_info >= (3, 7):
        return asyncio.current_task(loop=loop)
    else:
        return asyncio.Task.current_task(loop=loop)


def get_event_loop() -> asyncio.AbstractEventLoop:
    """get event loop in runtime"""
    if sys.version_info >= (3, 7):
        return asyncio.get_running_loop()

    loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()
    if not loop.is_running():
        raise RuntimeError("no running event loop")
    return loop


def done_future(loop: Optional[asyncio.AbstractEventLoop] = None) -> asyncio.Future:
    """create init future, use in obj.__inti__ method"""
    future: asyncio.Future = asyncio.Future(loop=loop)
    future.set_result(True)
    return future


async def can_cancel_sleep(delay: float, *, loop: Optional[asyncio.AbstractEventLoop] = None) -> None:
    """Sleep method that can be cancelled"""
    await asyncio.wait_for(asyncio.Future(), delay, loop=loop)  # type: ignore


async def as_first_completed(
    future_list: List[Union[Coroutine, asyncio.Future]],
    not_cancel_future_list: Optional[List[Union[Coroutine, asyncio.Future]]] = None,
) -> Any:
    """Wait for multiple coroutines to process data, until one of the coroutines returns data,
    and the remaining coroutines are cancelled

    The following two examples are equivalent
    >>> await as_first_completed([asyncio.Future(), asyncio.shield(asyncio.Future())])
    >>> await as_first_completed([asyncio.Future()], not_cancel_future_list=[asyncio.Future()])
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
