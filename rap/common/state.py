import asyncio
import logging
import time
from typing import Any, Callable, Dict, Optional, Set

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
        self, interval: int = 1, change_callback_set: Optional[Set[Callable]] = None, change_callback_wait_cnt: int = 3
    ) -> None:
        self._change_callback_set: Set[Callable] = change_callback_set or set()
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

    def set_value(self, key: Any, value: Any) -> None:
        self._future_dict[key] = value

    def get_value(self, key: Any) -> Any:
        try:
            return self._dict[key]
        except KeyError:
            raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{key}'")

    def __len__(self) -> int:
        return len(self._dict)

    def _update_change_callback_future_run_timestamp(self) -> None:
        self._change_callback_future_run_timestamp = self._loop.time()  # type: ignore

    def _change_state(self) -> None:
        loop: asyncio.AbstractEventLoop = self._loop  # type: ignore
        self._timestamp = loop.time()
        logging.debug("%s run once at loop time: %s" % (self.__class__.__name__, self._timestamp))
        self._dict = self._future_dict
        self._future_dict = {}
        if self._change_callback_set:
            if self._change_callback_future and not self._change_callback_future.done():
                if (
                    self._change_callback_future_run_timestamp + self._interval * self._change_callback_wait_cnt
                    < self._timestamp
                ):
                    loop.call_at(self._timestamp + self._interval, self._change_state)
                    return
                elif self._change_callback_future.cancelled():
                    self._change_callback_future.cancel()

            self._change_callback_future = asyncio.gather(*[fn() for fn in self._change_callback_set])
            self._change_callback_future.add_done_callback(lambda f: self._update_change_callback_future_run_timestamp)
        loop.call_at(self._timestamp + self._interval, self._change_state)

    def add_callback(self, fn: Callable) -> None:
        self._change_callback_set.add(fn)

    def remove_callback(self, fn: Callable) -> None:
        self._change_callback_set.remove(fn)

    def change_state(self) -> None:
        if self._is_closed:
            if not self._loop:
                self._loop = get_event_loop()
            self._timestamp = self._loop.time()
            self._loop.call_later(self._interval, self._change_state)
            self._is_closed = True
        else:
            raise RuntimeError(f"{self.__class__.__name__} already run")
