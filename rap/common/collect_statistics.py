import asyncio
import logging
import time
from functools import partial
from threading import Lock
from typing import Any, Awaitable, Callable, Dict, List, Optional, Set, Tuple

from rap.common.cache import Cache
from rap.common.utils import get_event_loop


#############
#  Metric  #
###########
# design like prometheus, url:https://prometheus.io/docs/concepts/metric_types/"
class Metric(object):
    def __init__(self, name: str):
        self.name: str = name
        self.diff: int = 0

    def set_value(self, value: float) -> None:
        raise RuntimeError("Not Implemented")

    def get_value(self) -> float:
        raise RuntimeError("Not Implemented")


class Counter(Metric):
    """
    copy from prometheus doc
    A counter is a cumulative metric that represents a single monotonically increasing counter
     whose value can only increase or be reset to zero on restart.
     For example, you can use a counter to represent the number of requests served, tasks completed, or errors.
    """


class Gauge(Metric):
    """
    copy from prometheus doc
    A gauge is a metric that represents a single numerical value that can arbitrarily go up and down.

    Gauges are typically used for measured values like temperatures or current memory usage,
     but also "counts" that can go up and down, like the number of concurrent requests.
    """

    def __init__(self, name: str, diff: int = 0):
        super().__init__(name)
        self.diff = diff

    def increment(self, value: float = 1.0) -> None:
        self.set_value(value)

    def decrement(self, value: float = 1.0) -> None:
        self.set_value(-value)


class WindowStatistics(object):
    """Collect data using time sliding window principle"""

    def __init__(
        self,
        interval: Optional[int] = None,
        max_interval: Optional[int] = None,
        metric_cache: Optional[Cache] = None,
        callback_interval: Optional[int] = None,
        callback_set: Optional[Set[Callable[[dict], None]]] = None,
        callback_priority_set: Optional[Set[Callable[[dict], None]]] = None,
        callback_wait_cnt: int = 3,
    ) -> None:
        """
        :param interval: Interval for switching statistics buckets
        :param callback_set: The callback set when the bucket is switched,
            the parameter of the callback is the data of the available bucket.
            Data can be obtained through this callback
        :param callback_priority_set: The callback set when the bucket is switched,
            the parameter of the callback is the data of the available bucket.
            The priority of this set is relatively high, it is used to write some statistical data
        :param callback_wait_cnt: The maximum number of cycles allowed for the callback collection to take time.
            For example, the time of a cycle is 2 seconds, and the maximum number of cycles allowed is 3,
            then the maximum execution time of the callback set is 6 seconds
        """
        self._interval: int = interval or 1
        self._max_interval: int = max_interval or 60
        self._callback_interval: int = callback_interval or self._max_interval // 2
        self._callback_set: Set[Callable] = callback_set or set()
        self._callback_priority_set: Set[Callable] = callback_priority_set or set()
        self._callback_wait_cnt: int = callback_wait_cnt
        self._bucket_len: int = (self._max_interval // self._interval) + 5
        self._bucket_list: List[dict] = [{"cnt": 0} for _ in range(self._bucket_len)]
        self._metric_cache: Cache = metric_cache or Cache(self._max_interval)
        self._start_timestamp: int = int(time.time() * 1000)
        self._loop_timestamp: float = time.time()

        self._is_closed: bool = True
        self.statistics_dict: dict = {}
        self._look: Lock = Lock()
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._change_callback_future: Optional[asyncio.Future] = None
        self._change_callback_future_run_timestamp: float = time.time()

    def registry_metric(self, key: Any, metric: Metric, expire: float) -> None:
        self._metric_cache.add(key, expire, metric)
        if isinstance(metric, Gauge):
            get_value_fn: Callable = partial(self._get_gauge_value, key)
            set_value_fn: Callable = partial(self._set_gauge_value, key)
        else:
            get_value_fn = partial(self._get_count_value, key, diff=metric.diff)
            set_value_fn = partial(self._set_count_value, key)

        setattr(metric, "_temp_get_value", metric.get_value)
        setattr(metric, "_temp_set_value", metric.set_value)
        setattr(metric, metric.get_value.__name__, get_value_fn)
        setattr(metric, metric.set_value.__name__, set_value_fn)

    def drop_metric(self, key: Any) -> None:
        metric: Optional[Metric] = self._metric_cache.pop(key)
        if metric:
            setattr(metric, metric.get_value.__name__, getattr(metric, "_temp_get_value"))
            setattr(metric, metric.set_value.__name__, getattr(metric, "_temp_set_value"))

    @property
    def is_closed(self) -> bool:
        return self._is_closed

    def _get_now_info(self) -> Tuple[int, int]:
        """return now timestamp's (Quotient, remainder)"""
        now_timestamp: int = int(time.time() * 1000)
        diff: int = now_timestamp - self._start_timestamp
        return divmod(diff, self._bucket_len)

    def _set_gauge_value(self, key: Any, value: float) -> None:
        cnt, index = self._get_now_info()
        bucket: dict = self._bucket_list[index]
        with self._look:
            bucket["cnt"] = cnt
            bucket[key] = value

    def _set_count_value(self, key: Any, value: float) -> None:
        cnt, index = self._get_now_info()
        bucket: dict = self._bucket_list[index]
        with self._look:
            bucket["cnt"] = cnt
            if key not in self._bucket_list:
                bucket[key] = value
            else:
                bucket[key] += value

    def set_value(self, key: Any, value: float) -> None:
        if isinstance(self._metric_cache.get(key), Gauge):
            self._set_gauge_value(key, value)
        else:
            self._set_count_value(key, value)

    def _get_gauge_value(self, key: Any) -> float:
        cnt, index = self._get_now_info()
        bucket: dict = self._bucket_list[index - 1]
        if bucket["cnt"] != cnt:
            raise KeyError(key)
        return bucket.get(key, 0.0)

    def _get_count_value(self, key: Any, diff: int = 0) -> float:
        cnt, index = self._get_now_info()
        value: float = 0.0
        for i in range(diff):
            bucket: dict = self._bucket_list[index - i - 1]
            if bucket["cnt"] != cnt:
                break
            value += bucket.get(key, 0.0)
        return value

    def get_value(self, key: Any, diff: int = 0) -> float:
        assert diff <= self._max_interval
        if isinstance(self._metric_cache.get(key), Gauge):
            return self._get_gauge_value(key)
        else:
            return self._get_count_value(key)

    def _update_change_callback_future_run_timestamp(self) -> None:
        self._change_callback_future_run_timestamp = self._loop.time()  # type: ignore

    def _run_callback(self) -> asyncio.Future:
        loop: asyncio.AbstractEventLoop = self._loop  # type: ignore

        self.statistics_dict = {}
        for key, metric in self._metric_cache.items():
            self.statistics_dict[key] = metric.get_value()

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
            await asyncio.gather(*[_safe_run_callback(fn, self.statistics_dict) for fn in self._callback_priority_set])
            # change callback dict is read only
            copy_dict: dict = self.statistics_dict.copy()
            await asyncio.gather(*[_safe_run_callback(fn, copy_dict) for fn in self._callback_set])

        return asyncio.ensure_future(_real_run_callback())

    def _change_state(self) -> None:
        if self._is_closed:
            return
        loop: asyncio.AbstractEventLoop = self._loop  # type: ignore
        self._loop_timestamp = loop.time()
        logging.debug("%s run once at loop time: %s" % (self.__class__.__name__, self._start_timestamp))

        # call callback
        if self._callback_set or self._callback_priority_set:
            if self._change_callback_future and not self._change_callback_future.done():
                # The callback is still executing
                if (
                    self._change_callback_future_run_timestamp + self._interval * self._callback_wait_cnt
                    < self._start_timestamp
                ):
                    # the running time does not exceed the maximum allowable number of cycles
                    loop.call_at(self._start_timestamp + self._interval, self._change_state)
                    return
                elif not self._change_callback_future.cancelled():
                    logging.warning("Callback collection execution timeout...cancel")
                    self._change_callback_future.cancel()

            self._change_callback_future = self._run_callback()
            self._change_callback_future.add_done_callback(lambda f: self._update_change_callback_future_run_timestamp)
        loop.call_at(self._start_timestamp + self._interval, self._change_state)

    def _check_run(self) -> None:
        if not self.is_closed:
            raise RuntimeError(f"Operation failed, {self.__class__.__name__} is running.")

    def add_callback(self, fn: Callable) -> None:
        self._check_run()
        self._callback_set.add(fn)

    def remove_callback(self, fn: Callable) -> None:
        self._check_run()
        self._callback_set.remove(fn)

    def add_priority_callback(self, fn: Callable) -> None:
        self._check_run()
        self._callback_priority_set.add(fn)

    def remove_priority_callback(self, fn: Callable) -> None:
        self._check_run()
        self._callback_priority_set.remove(fn)

    def close(self) -> None:
        self._is_closed = True

    def change_state(self) -> None:
        if self._is_closed:
            self._loop = get_event_loop()
            self._loop_timestamp = self._loop.time()
            self._change_callback_future_run_timestamp = self._loop.time()
            self._is_closed = False
            self._loop.call_later(self._interval, self._change_state)
        else:
            raise RuntimeError(f"{self.__class__.__name__} already run")
