import asyncio
import inspect
import sys
import time
from contextvars import ContextVar, Token
from types import TracebackType
from typing import Any, Callable, Coroutine, Dict, List, Optional, Type, Union


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
    await asyncio.wait_for(asyncio.Future(), delay, loop=loop)


def gen_new_param_coro(coro: Coroutine, new_param_dict: Dict[str, Any]) -> Coroutine:
    """Return a new coro according to the parameters
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
    """Compared with the original version, an additional method `inflight` is used to obtain the current usage"""

    def __init__(self, value: int = 1, *, loop: Optional[asyncio.AbstractEventLoop] = None):
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


class IgnoreDeadlineTimeoutExc(Exception):
    pass


deadline_context: ContextVar[Optional["Deadline"]] = ContextVar("deadline_context", default=None)


class Deadline(object):
    """
    cancel and timeout for human
     The design is inspired by https://vorpus.org/blog/timeouts-and-cancellation-for-humans/
    """

    def __init__(
        self,
        delay: Optional[float],
        loop: Optional[asyncio.AbstractEventLoop] = None,
        timeout_exc: Optional[Exception] = None,
    ):
        """
        :param delay: How many seconds are before the deadline, if delay is None, Deadline not delay
        :param loop: Event loop
        :param timeout_exc: The exception thrown when the task is not completed at the deadline
            None: raise asyncio.Timeout
            IgnoreDeadlineTimeoutExc: not raise exc
        """
        self._delay: Optional[float] = delay
        self._loop = loop or get_event_loop()
        self._timeout_exc: Exception = timeout_exc or asyncio.TimeoutError()

        self._parent: Optional["Deadline"] = None
        self._child: Optional["Deadline"] = None
        self._deadline_future: asyncio.Future = asyncio.Future()
        self._with_scope_future: Optional[asyncio.Future] = None
        if self._delay is not None:
            self._end_timestamp: Optional[float] = time.time() + self._delay
            self._end_loop_time: Optional[float] = self._loop.time() + self._delay
            self._loop.call_at(self._end_loop_time, self._set_deadline_future_result)
        else:
            self._end_timestamp = None
            self._end_loop_time = None

        self._context_token: Optional[Token] = None

    def _set_context(self) -> None:
        """reset parent context and set self context"""
        if self._parent and self._parent._context_token:
            deadline_context.reset(self._parent._context_token)
            self._parent._context_token = None
        self._context_token = deadline_context.set(self)

    def _reset_context(self) -> None:
        """reset self context and set parent (if active) context"""
        if self._context_token:
            deadline_context.reset(self._context_token)
            self._context_token = None
        if self._parent and self._parent.is_active:
            self._parent._context_token = deadline_context.set(self._parent)

    def _set_deadline_future_result(self) -> None:
        """set deadline finish"""
        self._deadline_future.set_result(True)
        if self._with_scope_future and not self._with_scope_future.cancelled():
            self._with_scope_future.cancel()

    def __await__(self) -> Any:
        """wait deadline"""
        return self._deadline_future.__await__()

    #######################
    # support `async with`#
    #######################
    async def __aenter__(self) -> "Deadline":
        return self.__enter__()

    async def __aexit__(
        self, exc_type: Optional[Type[BaseException]], exc_val: Optional[BaseException], exc_tb: Optional[TracebackType]
    ) -> Optional[bool]:
        return self.__exit__(exc_type, exc_val, exc_tb)

    def __enter__(self) -> "Deadline":
        if self._with_scope_future:
            raise RuntimeError("`with` can only be called once")
        self._set_context()
        if self._delay is not None:
            main_task: Optional[asyncio.Task] = current_task(self._loop)
            if not main_task:
                raise RuntimeError("Can not found current task")
            self._with_scope_future = main_task
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> Optional[bool]:
        try:
            if self._with_scope_future:
                self._with_scope_future = None
            else:
                return None
            if self._deadline_future.done():
                if exc_type:
                    if isinstance(self._timeout_exc, IgnoreDeadlineTimeoutExc):
                        return True
                raise self._timeout_exc
            else:
                return None
        finally:
            self._reset_context()

    def inherit(self, timeout_exc: Optional[Exception] = None) -> "Deadline":
        """gen child Deadline"""
        if not timeout_exc:
            timeout_exc = self._timeout_exc

        if self._end_loop_time is None:
            delay: Optional[float] = None
        else:
            delay = self._end_loop_time - self._loop.time()

        deadline: "Deadline" = self.__class__(delay=delay, loop=self._loop, timeout_exc=timeout_exc)
        self._child = deadline
        deadline._parent = self
        return deadline

    @property
    def is_active(self) -> bool:
        return self._with_scope_future is not None

    @property
    def surplus(self) -> float:
        if self._end_loop_time is None:
            return 0.0
        return self._end_loop_time - self._loop.time()

    @property
    def end_timestamp(self) -> Optional[float]:
        return self._end_timestamp

    @property
    def end_loop_time(self) -> Optional[float]:
        return self._end_loop_time

    async def wait_for(self, future: Union[asyncio.Future, Coroutine]) -> Any:
        """wait future completed or deadline"""
        try:
            if self._delay is None:
                return await future
            else:
                return await asyncio.wait_for(future, self.surplus)
        except asyncio.TimeoutError:
            if isinstance(self._timeout_exc, IgnoreDeadlineTimeoutExc):
                return
            else:
                raise self._timeout_exc
