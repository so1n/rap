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
    prefix: str = ""

    def __init__(self, name: str):
        self.name: str = name
        self.metric_cache_name: str = self.gen_metric_cache_name(name)
        self.diff: int = 0

    @classmethod
    def gen_metric_name(cls, name: str) -> str:
        return f"{cls.prefix}_{name}"

    @classmethod
    def gen_metric_cache_name(cls, name: str) -> str:
        return f"metric_{cls.prefix}_{name}"

    def set_value(self, *args: Any, **kwargs: Any) -> None:
        raise RuntimeError("Not Implemented")

    def get_value(self) -> float:
        raise RuntimeError("Not Implemented")

    def get_statistics_value(self) -> float:
        raise RuntimeError("Not Implemented")


class Counter(Metric):
    """
    copy from prometheus doc
    A counter is a cumulative metric that represents a single monotonically increasing counter
     whose value can only increase or be reset to zero on restart.
     For example, you can use a counter to represent the number of requests served, tasks completed, or errors.
    """

    prefix: str = "counter"

    def increment(self, value: float = 1.0) -> None:
        self.set_value(value, is_cover=False)

    def decrement(self, value: float = 1.0) -> None:
        self.set_value(-value, is_cover=False)


class Gauge(Metric):
    """
    copy from prometheus doc
    A gauge is a metric that represents a single numerical value that can arbitrarily go up and down.

    Gauges are typically used for measured values like temperatures or current memory usage,
     but also "counts" that can go up and down, like the number of concurrent requests.
    """

    prefix: str = "gauge"

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
        # window interval param
        interval: Optional[int] = None,
        max_interval: Optional[int] = None,
        # callback param
        statistics_interval: Optional[int] = None,
        statistics_callback_set: Optional[Set[Callable[[dict], None]]] = None,
        statistics_callback_priority_set: Optional[Set[Callable[[dict], None]]] = None,
        statistics_callback_wait_cnt: int = 3,
        metric_cache: Optional[Cache] = None,
    ) -> None:
        """
        :param interval: Minimum statistical period, default 1
        :param max_interval: Maximum statistical period(counter expire timestamp = max_interval + 5), default 60

        :param statistics_interval: call callback interval, default min(max_interval // 3, 10)
        :param statistics_callback_set: The callback set when the bucket is switched,
            the parameter of the callback is the data of the available bucket.
            Data can be obtained through this callback
        :param statistics_callback_priority_set: The callback set when the bucket is switched,
            the parameter of the callback is the data of the available bucket.
            The priority of this set is relatively high, it is used to write some statistical data
        :param statistics_callback_wait_cnt:
            The maximum number of cycles allowed for the callback collection to take time.
             For example, the time of a cycle is 2 seconds, and the maximum number of cycles allowed is 3,
             then the maximum execution time of the callback set is 6 seconds
        :param metric_cache:
            Specify the Cache instance that comes with rap.
             In order to make the statistics faster,
             it is not recommended to share the Cache instance with other services
        """
        self._interval: int = interval or 1
        self._max_interval: int = max_interval or 60
        self._statistics_interval: int = statistics_interval or min((self._max_interval - self._interval) // 3, 10)
        self._statistics_callback_set: Set[Callable] = statistics_callback_set or set()
        self._statistics_callback_priority_set: Set[Callable] = statistics_callback_priority_set or set()
        self._statistics_callback_wait_cnt: int = statistics_callback_wait_cnt
        self._metric_cache: Cache = metric_cache or Cache()

        self._gauge_bucket_len: int = (self._max_interval // self._interval) + 5
        self._gauge_bucket_list: List[Dict[str, float]] = [{"cnt": -1} for _ in range(self._gauge_bucket_len)]
        self._start_timestamp: int = int(time.time())
        self._loop_timestamp: float = 0.0
        self._is_closed: bool = True
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._statistics_callback_future: Optional[asyncio.Future] = None
        self._statistics_callback_future_run_timestamp: float = time.time()

        self.statistics_dict: Dict[str, float] = {}

    ##########
    # Metric #
    ##########
    def registry_metric(self, metric: Metric, expire: Optional[float] = None) -> None:
        key: str = metric.metric_cache_name
        if key in self._metric_cache:
            cache_value: Optional[Metric] = self._metric_cache.get(key, None)
            if cache_value and cache_value is not metric:
                raise ValueError("different metric")

        if not expire:
            expire = -1
        self._metric_cache.add(key, expire, metric)
        if isinstance(metric, Gauge):
            if not metric.diff <= self._max_interval:
                raise ValueError(f"metric.{metric.name}.diff > {self._max_interval}")
            get_value_fn: Callable = partial(self.get_gauge_value, metric.name, diff=metric.diff)
            set_value_fn: Callable = partial(self._set_gauge_value, metric.name)
        else:
            get_value_fn = partial(self.get_counter_value, metric.name)
            set_value_fn = partial(self._set_counter_value, metric.name)

        setattr(metric, "_temp_get_value", metric.get_value)
        setattr(metric, "_temp_set_value", metric.set_value)
        setattr(metric, "_temp_get_statistics_value", metric.get_statistics_value)
        setattr(metric, metric.get_value.__name__, get_value_fn)
        setattr(metric, metric.set_value.__name__, set_value_fn)
        setattr(metric, metric.get_statistics_value.__name__, partial(self.get_statistics_value, metric.name))

    def drop_metric(self, key: str) -> None:
        metric: Optional[Metric] = self._metric_cache.pop(key)
        if metric:
            setattr(metric, metric.get_statistics_value.__name__, getattr(metric, "_temp_get_statistics_value"))
            setattr(metric, metric.get_value.__name__, getattr(metric, "_temp_get_value"))
            setattr(metric, metric.set_value.__name__, getattr(metric, "_temp_set_value"))

    ################
    # gauge metric #
    ################
    def get_statistics_value(self, key: str) -> float:
        return self.statistics_dict.get(key, 0.0)

    def _get_now_info(self) -> Tuple[int, int]:
        """return now timestamp's (Quotient, remainder)"""
        now_timestamp: int = int(time.time())
        diff: int = now_timestamp - self._start_timestamp
        return divmod(diff, self._gauge_bucket_len)

    def _set_gauge_value(self, key: str, value: float = 1) -> None:
        cnt, index = self._get_now_info()
        bucket: dict = self._gauge_bucket_list[index]
        bucket["cnt"] = cnt
        if key not in bucket:
            bucket[key] = value
        else:
            bucket[key] += value

    def set_gauge_value(self, key: str, expire: float, diff: int = 1, value: float = 1) -> None:
        cache_key: str = Gauge.gen_metric_cache_name(key)
        if cache_key not in self._metric_cache:
            self.registry_metric(Gauge(key, diff=diff), expire)
        self._set_gauge_value(key, value)

    def get_gauge_value(self, key: str, diff: int = 0) -> float:
        cache_key: str = Gauge.gen_metric_cache_name(key)
        assert cache_key in self._metric_cache
        assert diff <= self._max_interval
        cnt, index = self._get_now_info()
        value: float = 0.0
        for i in range(diff):
            bucket: dict = self._gauge_bucket_list[index - i - 1]
            if bucket["cnt"] != cnt:
                break
            value += bucket.get(key, 0.0)
        return value

    ################
    # count metric #
    ################
    def _set_counter_value(self, key: str, value: float, is_cover: bool = True) -> None:
        assert value > 0, "counter value must > 0"
        key = f"statistics:{key}"
        if is_cover:
            self._metric_cache.add(key, self._max_interval + 5, value)
        else:
            self._metric_cache.add(key, self._max_interval + 5, self.get_counter_value(key) + value)

    def set_counter_value(self, key: str, expire: float, value: float = 1, is_cover: bool = True) -> None:
        cache_key: str = Gauge.gen_metric_cache_name(key)
        if cache_key not in self._metric_cache:
            self.registry_metric(Counter(key), expire)
        self._set_counter_value(key, value, is_cover=is_cover)

    def get_counter_value(self, key: str) -> float:
        key = f"statistics:{key}"
        assert key in self._metric_cache
        return self._metric_cache.get(key, 0.0)

    #######################
    # statistics callback #
    #######################
    def _update_statistics_callback_future_run_timestamp(self) -> None:
        self._statistics_callback_future_run_timestamp = self._loop.time()  # type: ignore

    def _run_callback(self) -> asyncio.Future:
        loop: asyncio.AbstractEventLoop = self._loop  # type: ignore

        self.statistics_dict = {}
        for key, metric in self._metric_cache.items():
            if isinstance(metric, Metric):
                self.statistics_dict[metric.name] = metric.get_value()

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
            # statistics callback is serial execution, no need to use lock
            await asyncio.gather(
                *[_safe_run_callback(fn, self.statistics_dict) for fn in self._statistics_callback_priority_set]
            )
            # statistics callback dict is read only
            copy_dict: dict = self.statistics_dict.copy()
            await asyncio.gather(*[_safe_run_callback(fn, copy_dict) for fn in self._statistics_callback_set])

        return asyncio.ensure_future(_real_run_callback())

    def _statistics_data(self) -> None:
        if self._is_closed:
            return
        loop: asyncio.AbstractEventLoop = self._loop  # type: ignore
        self._loop_timestamp = loop.time()
        logging.debug("%s run once at loop time: %s" % (self.__class__.__name__, self._start_timestamp))
        # call callback
        if self._statistics_callback_set or self._statistics_callback_priority_set:
            if self._statistics_callback_future and not self._statistics_callback_future.done():
                # The callback is still executing
                if (
                    self._statistics_callback_future_run_timestamp
                    + self._statistics_interval * self._statistics_callback_wait_cnt
                    < self._start_timestamp
                ):
                    # the running time does not exceed the maximum allowable number of cycles
                    loop.call_at(self._start_timestamp + self._statistics_interval, self._statistics_data)
                    return
                elif not self._statistics_callback_future.cancelled():
                    logging.warning("Callback collection execution timeout...cancel")
                    self._statistics_callback_future.cancel()

            self._statistics_callback_future = self._run_callback()
            self._statistics_callback_future.add_done_callback(
                lambda f: self._update_statistics_callback_future_run_timestamp
            )
        loop.call_at(self._start_timestamp + self._statistics_interval, self._statistics_data)

    def add_callback(self, fn: Callable) -> None:
        self._statistics_callback_set.add(fn)

    def remove_callback(self, fn: Callable) -> None:
        self._statistics_callback_set.remove(fn)

    def add_priority_callback(self, fn: Callable) -> None:
        self._statistics_callback_priority_set.add(fn)

    def remove_priority_callback(self, fn: Callable) -> None:
        self._statistics_callback_priority_set.remove(fn)

    def close(self) -> None:
        self._is_closed = True

    def statistics_data(self) -> None:
        if self._is_closed:
            self._loop = get_event_loop()
            self._loop_timestamp = self._loop.time()
            self._statistics_callback_future_run_timestamp = self._loop.time()
            self._is_closed = False
            self._loop.call_later(self._statistics_interval, self._statistics_data)
        else:
            raise RuntimeError(f"{self.__class__.__name__} already run")

    @property
    def is_closed(self) -> bool:
        return self._is_closed


class ThreadWindowStatistics(WindowStatistics):
    def __init__(
        self,
        # window interval param
        interval: Optional[int] = None,
        max_interval: Optional[int] = None,
        # callback param
        statistics_interval: Optional[int] = None,
        statistics_callback_set: Optional[Set[Callable[[dict], None]]] = None,
        statistics_callback_priority_set: Optional[Set[Callable[[dict], None]]] = None,
        statistics_callback_wait_cnt: int = 3,
        metric_cache: Optional[Cache] = None,
    ) -> None:
        super().__init__(
            interval=interval,
            max_interval=max_interval,
            statistics_interval=statistics_interval,
            statistics_callback_set=statistics_callback_set,
            statistics_callback_priority_set=statistics_callback_priority_set,
            statistics_callback_wait_cnt=statistics_callback_wait_cnt,
            metric_cache=metric_cache,
        )
        self._gauge_look: Lock = Lock()
        self._counter_look: Lock = Lock()

    def _set_counter_value(self, key: str, value: float, is_cover: bool = True) -> None:
        with self._counter_look:
            super()._set_counter_value(key, value, is_cover)

    def _set_gauge_value(self, key: str, value: float = 1) -> None:
        with self._gauge_look:
            super(ThreadWindowStatistics, self)._set_gauge_value(key, value)
