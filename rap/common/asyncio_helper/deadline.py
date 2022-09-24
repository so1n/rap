import asyncio
import time
from contextvars import ContextVar, Token
from types import TracebackType
from typing import Any, Coroutine, Optional, Type, Union

from .util import current_task, get_event_loop


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
                if exc_type and isinstance(self._timeout_exc, IgnoreDeadlineTimeoutExc):
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


def get_deadline(
    delay: Optional[float],
    loop: Optional[asyncio.AbstractEventLoop] = None,
    timeout_exc: Optional[Exception] = None,
) -> Deadline:
    deadline: Optional[Deadline] = deadline_context.get()
    if not deadline:
        deadline = Deadline(delay, loop=loop, timeout_exc=timeout_exc)
    return deadline
