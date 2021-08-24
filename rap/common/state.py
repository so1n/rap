import asyncio
import logging
import time
from dataclasses import MISSING
from functools import partial
from threading import Lock
from typing import Any, Awaitable, Callable, Dict, Optional, Set

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
    """Collect data using time sliding window principle"""

    def __init__(
        self,
        interval: int = 1,
        change_callback_set: Optional[Set[Callable[[dict], None]]] = None,
        change_callback_priority_set: Optional[Set[Callable[[dict], None]]] = None,
        change_callback_wait_cnt: int = 3,
    ) -> None:
        """
        :param interval: Interval for switching statistics buckets
        :param change_callback_set: The callback set when the bucket is switched,
            the parameter of the callback is the data of the available bucket.
            Data can be obtained through this callback
        :param change_callback_priority_set: The callback set when the bucket is switched,
            the parameter of the callback is the data of the available bucket.
            The priority of this set is relatively high, it is used to write some statistical data
        :param change_callback_wait_cnt: The maximum number of cycles allowed for the callback collection to take time.
            For example, the time of a cycle is 2 seconds, and the maximum number of cycles allowed is 3,
            then the maximum execution time of the callback set is 6 seconds
        """
        self._change_callback_set: Set[Callable] = change_callback_set or set()
        self._change_callback_priority_set: Set[Callable] = change_callback_priority_set or set()
        self._change_callback_wait_cnt: int = change_callback_wait_cnt
        self._interval: int = interval
        self._timestamp: float = time.time()
        self._is_closed: bool = True

        self._look: Lock = Lock()
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._future_dict: dict = {}
        self._dict: dict = {}
        self._change_callback_future: Optional[asyncio.Future] = None
        self._change_callback_future_run_timestamp: float = time.time()

    @property
    def is_closed(self) -> bool:
        return self._is_closed

    def increment(self, key: Any, value: int = 1) -> None:
        with self._look:
            if key not in self._future_dict:
                self._future_dict[key] = value
            else:
                self._future_dict[key] += value

    def decrement(self, key: Any, value: int = 1) -> None:
        with self._look:
            if key not in self._future_dict:
                self._future_dict[key] = -value
            else:
                self._future_dict[key] -= value

    def set_value(self, key: Any, value: Any) -> None:
        self._future_dict[key] = value

    def get_value(self, key: Any, default_value: Any = MISSING) -> Any:
        try:
            return self._dict[key]
        except KeyError:
            if default_value is not MISSING:
                return default_value
            raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{key}'")

    def __len__(self) -> int:
        return len(self._dict)

    def _update_change_callback_future_run_timestamp(self) -> None:
        self._change_callback_future_run_timestamp = self._loop.time()  # type: ignore

    def _run_callback(self) -> asyncio.Future:
        loop: asyncio.AbstractEventLoop = self._loop  # type: ignore

        async def _safe_run_callback(fn: Callable, dict_param: Dict) -> None:
            if asyncio.iscoroutinefunction(fn):
                coro: Awaitable = fn(dict_param)
            else:
                coro = loop.run_in_executor(None, partial(fn, dict_param))
            try:
                await coro
            except Exception as e:
                logging.exception(f"{self.__class__.__name__} run {fn} error: {e}")

        async def _real_run_callback() -> None:
            # change callback is serial execution, no need to use lock
            await asyncio.gather(*[_safe_run_callback(fn, self._dict) for fn in self._change_callback_priority_set])
            # change callback dict is read only
            copy_dict: dict = self._dict.copy()
            await asyncio.gather(*[_safe_run_callback(fn, copy_dict) for fn in self._change_callback_set])

        return asyncio.ensure_future(_real_run_callback())

    def _change_state(self) -> None:
        if self._is_closed:
            return
        loop: asyncio.AbstractEventLoop = self._loop  # type: ignore
        self._timestamp = loop.time()
        logging.debug("%s run once at loop time: %s" % (self.__class__.__name__, self._timestamp))
        # replace dict
        self._dict = self._future_dict
        self._future_dict = {}

        # call callback
        if self._change_callback_set or self._change_callback_priority_set:
            if self._change_callback_future and not self._change_callback_future.done():
                # The callback is still executing
                if (
                    self._change_callback_future_run_timestamp + self._interval * self._change_callback_wait_cnt
                    < self._timestamp
                ):
                    # the running time does not exceed the maximum allowable number of cycles
                    loop.call_at(self._timestamp + self._interval, self._change_state)
                    return
                elif not self._change_callback_future.cancelled():
                    logging.warning("Callback collection execution timeout...cancel")
                    self._change_callback_future.cancel()

            self._change_callback_future = self._run_callback()
            self._change_callback_future.add_done_callback(lambda f: self._update_change_callback_future_run_timestamp)
        loop.call_at(self._timestamp + self._interval, self._change_state)

    def _check_run(self) -> None:
        if not self.is_closed:
            raise RuntimeError(f"Operation failed, {self.__class__.__name__} is running.")

    def add_callback(self, fn: Callable) -> None:
        self._check_run()
        self._change_callback_set.add(fn)

    def remove_callback(self, fn: Callable) -> None:
        self._check_run()
        self._change_callback_set.remove(fn)

    def add_priority_callback(self, fn: Callable) -> None:
        self._check_run()
        self._change_callback_priority_set.add(fn)

    def remove_priority_callback(self, fn: Callable) -> None:
        self._check_run()
        self._change_callback_priority_set.remove(fn)

    def close(self) -> None:
        self._is_closed = True

    def change_state(self) -> None:
        if self._is_closed:
            self._loop = get_event_loop()
            self._timestamp = self._loop.time()
            self._change_callback_future_run_timestamp = self._loop.time()
            self._is_closed = False
            self._loop.call_later(self._interval, self._change_state)
        else:
            raise RuntimeError(f"{self.__class__.__name__} already run")
