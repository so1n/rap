import asyncio
import logging
import time
from functools import partial
from typing import Any, Awaitable, Callable, Dict, List, Optional, Set

from rap.common.utils import get_event_loop


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

    def __len__(self) -> int:
        return len(self._state)

    def __delattr__(self, key: Any) -> None:
        del self._state[key]


class WindowState(object):
    def __init__(
        self,
        interval: int = 1,
        change_callback_set: Optional[Set[Callable[[dict], None]]] = None,
        change_callback_priority_set: Optional[Set[Callable[[dict], None]]] = None,
        change_callback_wait_cnt: int = 3,
    ) -> None:
        self._change_callback_set: Set[Callable] = change_callback_set or set()
        self._change_callback_priority_set: Set[Callable] = change_callback_priority_set or set()
        self._change_callback_wait_cnt: int = change_callback_wait_cnt
        self._interval: int = interval
        self._timestamp: float = time.time()
        self._is_closed: bool = True

        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._future_dict: dict = {}
        self._dict: dict = {}
        self._change_callback_future: Optional[asyncio.Future] = None
        self._change_callback_future_run_timestamp: float = time.time()

    @property
    def is_closed(self) -> bool:
        return self._is_closed

    def increment(self, key: str, value: int = 1) -> None:
        if key not in self._future_dict:
            self._future_dict[key] = value
        else:
            self._future_dict[key] += value

    def decrement(self, key: str, value: int = 1) -> None:
        if key not in self._future_dict:
            self._future_dict[key] = -value
        else:
            self._future_dict[key] -= value

    def set_value(self, key: Any, value: Any) -> None:
        self._future_dict[key] = value

    def get_value(self, key: Any, default_value: Any = ...) -> Any:
        try:
            return self._dict[key]
        except KeyError:
            if default_value is not ...:
                return default_value
            raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{key}'")

    def __len__(self) -> int:
        return len(self._dict)

    def _update_change_callback_future_run_timestamp(self) -> None:
        self._change_callback_future_run_timestamp = self._loop.time()  # type: ignore

    def _run_callback(self) -> asyncio.Future:
        loop: asyncio.AbstractEventLoop = self._loop  # type: ignore

        async def _safe_run_callback(fn: Callable) -> None:
            if asyncio.iscoroutinefunction(fn):
                coro: Awaitable = fn(self._dict)
            else:
                coro = loop.run_in_executor(None, partial(fn, self._dict))
            try:
                await coro
            except Exception as e:
                logging.exception(f"{self.__class__.__name__} run {fn} error: {e}")

        async def _real_run_callback() -> None:
            coro_list: List[Awaitable] = []
            for fn in self._change_callback_priority_set:
                coro_list.append(_safe_run_callback(fn))
            await asyncio.gather(*coro_list)

            coro_list = []
            for fn in self._change_callback_set:
                coro_list.append(_safe_run_callback(fn))
            await asyncio.gather(*coro_list)

        return asyncio.ensure_future(_real_run_callback())

    def _change_state(self) -> None:
        if self._is_closed:
            return
        loop: asyncio.AbstractEventLoop = self._loop  # type: ignore
        self._timestamp = loop.time()
        logging.debug("%s run once at loop time: %s" % (self.__class__.__name__, self._timestamp))
        self._dict = self._future_dict
        self._future_dict = {}
        if self._change_callback_set or self._change_callback_priority_set:
            if self._change_callback_future and not self._change_callback_future.done():
                if (
                    self._change_callback_future_run_timestamp + self._interval * self._change_callback_wait_cnt
                    < self._timestamp
                ):
                    loop.call_at(self._timestamp + self._interval, self._change_state)
                    return
                elif self._change_callback_future.cancelled():
                    self._change_callback_future.cancel()

            self._change_callback_future = self._run_callback()
            self._change_callback_future.add_done_callback(lambda f: self._update_change_callback_future_run_timestamp)
        loop.call_at(self._timestamp + self._interval, self._change_state)

    def add_callback(self, fn: Callable) -> None:
        self._change_callback_set.add(fn)

    def remove_callback(self, fn: Callable) -> None:
        self._change_callback_set.remove(fn)

    def add_priority_callback(self, fn: Callable) -> None:
        self._change_callback_priority_set.add(fn)

    def remove_priority_callback(self, fn: Callable) -> None:
        self._change_callback_priority_set.remove(fn)

    def close(self) -> None:
        self._is_closed = True

    def change_state(self) -> None:
        if self._is_closed:
            if not self._loop:
                self._loop = get_event_loop()
            self._timestamp = self._loop.time()
            self._is_closed = False
            self._loop.call_later(self._interval, self._change_state)
        else:
            raise RuntimeError(f"{self.__class__.__name__} already run")
