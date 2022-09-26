import asyncio
import copy
import logging
import time
from functools import partial
from typing import Any, Callable, Coroutine, Dict, List, Optional, Set, Tuple, Union

from rap.common.asyncio_helper import get_event_loop, safe_del_future
from rap.common.cache import Cache

logger: logging.Logger = logging.getLogger(__name__)
_CallbackFn = Callable[[dict], Union[None, Coroutine[Any, Any, None]]]


#############
#  Metric  #
###########
# design like prometheus, url:https://prometheus.io/docs/concepts/metric_types/"
class Metric(object):
    prefix: str = ""
    ws: "WindowStatistics"

    def __init__(self, name: str):
        self.raw_name: str = name
        self.name: str = self.gen_metric_name(name)
        self.metric_cache_name: str = self.gen_metric_cache_name(name)
        self.diff: int = 0

    @classmethod
    def gen_metric_name(cls, name: str) -> str:
        return f"{cls.prefix}_{name}"

    @classmethod
    def gen_metric_cache_name(cls, name: str) -> str:
        return f"metric_{cls.prefix}_{name}"

    def set_value(self, *args: Any, **kwargs: Any) -> None:
        """Do not inherit this method, it is replaced when registered to Window Statistics"""
        raise RuntimeError("Not Implemented")

    def get_value(self) -> float:
        """Do not inherit this method, it is replaced when registered to Window Statistics"""
        raise RuntimeError("Not Implemented")

    def get_statistics_value(self) -> float:
        """Do not inherit this method, it is replaced when registered to Window Statistics"""
        raise RuntimeError("Not Implemented")


class Counter(Metric):
    """
    inherit from prometheus doc
    A counter is a cumulative metric that represents a single monotonically increasing counter
     whose value can only increase or be reset to zero on restart.
     For example, you can use a counter to represent the number of requests served, tasks completed, or errors.
    """

    prefix: str = "counter"

    def __init__(self, name: str, diff: int = 0):
        """diff: collection cycle，If diff<=0, then the value of diff is equal to max_interval interval
        The formula for calculating the statistical time is： max_interval / interval * diff
        """
        super().__init__(name)
        self.diff = diff

    def increment(self, value: float = 1.0) -> None:
        self.set_value(value, is_cover=False)

    def decrement(self, value: float = 1.0) -> None:
        self.set_value(-value, is_cover=False)


class Gauge(Metric):
    """
    inherit from prometheus doc
    A gauge is a metric that represents a single numerical value that can arbitrarily go up and down.

    Gauges are typically used for measured values like temperatures or current memory usage,
     but also "counts" that can go up and down, like the number of concurrent requests.
    """

    prefix: str = "gauge"

    def increment(self, value: float = 1.0) -> None:
        self.set_value(value, is_cover=False)

    def decrement(self, value: float = 1.0) -> None:
        self.set_value(-value, is_cover=False)


class WindowStatistics(object):
    """Collect data using time sliding window principle

    Storage Counter is designed to resemble a time wheel,
     `max interval` represents the total time of the time wheel,
     `max_interval / interval` represents how many grids the time wheel has

    The design of the storage Gauge is to use a cache with an expiration time
    """

    def __init__(
        self,
        # window interval param
        interval: Optional[int] = None,
        max_interval: Optional[int] = None,
        # callback param
        statistics_interval: Optional[int] = None,
        statistics_callback_set: Optional[Set[_CallbackFn]] = None,
        statistics_callback_priority_set: Optional[Set[_CallbackFn]] = None,
        statistics_callback_wait_cnt: Optional[int] = None,
        metric_cache: Optional[Cache] = None,
    ) -> None:
        """
        :param interval: Minimum statistical period, default 1(second)
        :param max_interval: Maximum statistical period(counter expire timestamp = max_interval + 5), default 60(second)

        :param statistics_interval: call callback interval, default min(max_interval // 3, 10)(second)
        :param statistics_callback_set: The callback set when the bucket is switched,
            the parameter of the callback is the data of the available bucket.
            Data can be obtained through this callback

            bucket data like: {metric1.name: 0.0, metric2.name: 1.0}
        :param statistics_callback_priority_set: The callback set when the bucket is switched,
            the parameter of the callback is the data of the available bucket.
            The priority of this set is relatively high, it is used to write some statistical data

            bucket data like: {metric1.name: 0.0, metric2.name: 1.0}
        :param statistics_callback_wait_cnt:
            The maximum number of cycles allowed to execute `statistics callback set`,
             for example, the execution cycle of a callback is 2 seconds, and the maximum number of cycles allowed is 3,
             then the maximum execution time of a callback is 6 seconds
        :param metric_cache:
            Specify the Cache instance that comes with rap.
             In order to make the statistics faster,
             it is not recommended to share the Cache instance with other services
        """
        self._interval: int = interval or 1
        self._max_interval: int = max_interval or 60
        self._statistics_interval: int = statistics_interval or min(int((self._max_interval - self._interval) // 3), 10)
        if self._statistics_interval <= 0:
            raise ValueError(
                f"statistics_interval must > 0,Please check the following parameters:max_interval:{self._max_interval};"
                f"internal:{self._interval};statistics_interval:{statistics_interval}"
            )
        self._statistics_callback_set: Set[_CallbackFn] = statistics_callback_set or set()
        self._statistics_callback_priority_set: Set[_CallbackFn] = statistics_callback_priority_set or set()
        self._statistics_callback_wait_cnt: int = statistics_callback_wait_cnt or 3
        self._metric_cache: Cache = metric_cache or Cache()

        self._bucket_len: int = (self._max_interval // self._interval) + 5
        self._bucket_list: List[Dict[str, float]] = [{"_metadata_cnt": -1} for _ in range(self._bucket_len)]
        self._start_timestamp: int = int(time.time())
        self._is_closed: bool = True
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._statistics_callback_future: Optional[asyncio.Future] = None
        self._statistics_callback_future_run_timestamp: float = time.time()

        self._statistics_dict: Dict[str, float] = {}

    @property
    def max_internal(self) -> int:
        return self._max_interval

    ##########
    # Metric #
    ##########
    def registry_metric(self, metric: Metric, expire: Optional[float] = None) -> None:
        """
        Register the Metric object, this method will replace part of the method implementation of the metric

        :param metric: Counter & Gauge metric
        :param expire: The expiration time of the metric, the default is not expired (second)
        """
        key: str = metric.metric_cache_name
        if key in self._metric_cache:
            cache_value: Optional[Metric] = self._metric_cache.get(key, None)
            if cache_value and cache_value is not metric:
                raise ValueError("different metric")

        self._metric_cache.add(key, expire or -1.0, metric)
        if isinstance(metric, Counter):
            if not (metric.diff <= self._max_interval):
                raise ValueError(f"metric.{metric.name}.diff > {self._max_interval}")
            if metric.diff <= 0:
                metric.diff = int(self._max_interval / self._interval)
            get_value_fn = partial(self.get_counter_value, metric.raw_name, diff=metric.diff)
            set_value_fn = partial(self._set_counter_value, metric.raw_name)
        else:
            get_value_fn: Callable = partial(self.get_gauge_value, metric.raw_name)
            set_value_fn: Callable = partial(self._set_gauge_value, metric.raw_name)

        setattr(metric, "_temp_get_value", metric.get_value)
        setattr(metric, "_temp_set_value", metric.set_value)
        setattr(metric, "_temp_get_statistics_value", metric.get_statistics_value)
        setattr(metric, metric.get_value.__name__, get_value_fn)
        setattr(metric, metric.set_value.__name__, set_value_fn)
        setattr(metric, metric.get_statistics_value.__name__, partial(self.get_statistics_value, metric.name))

    def drop_metric(self, key: str) -> None:
        """Remove the corresponding metric and restore its own method"""
        metric: Optional[Metric] = self._metric_cache.pop(key)
        if metric:
            setattr(metric, metric.get_statistics_value.__name__, getattr(metric, "_temp_get_statistics_value"))
            setattr(metric, metric.get_value.__name__, getattr(metric, "_temp_get_value"))
            setattr(metric, metric.set_value.__name__, getattr(metric, "_temp_set_value"))

    ##################
    # counter metric #
    ##################
    def get_statistics_value(self, key: str) -> float:
        return self._statistics_dict.get(key, 0.0)

    def _get_now_info(self) -> Tuple[int, int]:
        """return now timestamp's tuple(Quotient, remainder)"""
        now_timestamp: int = int(time.time())
        diff: int = now_timestamp - self._start_timestamp
        return divmod(diff, self._bucket_len)

    def _set_counter_value(self, key: str, value: float = 1, is_cover: bool = True) -> None:
        cnt, index = self._get_now_info()
        bucket: dict = self._bucket_list[index]
        bucket["_metadata_cnt"] = cnt
        key = Counter.gen_metric_name(key)
        if key not in bucket or is_cover:
            bucket[key] = value
        else:
            bucket[key] += value

    def set_counter_value(self, key: str, expire: float = -1.0, diff: int = 1, value: float = 1) -> Counter:
        """
        Set the value of Counter metric

        :param key: Counter key name
        :param expire: When the Counter expires from cache, if expire=-1.0 then it will never expire
        :param diff: collection cycle
            The formula for calculating the statistical time is： max_interval / interval * diff
        :param value: Counter value
        """
        cache_key: str = Counter.gen_metric_cache_name(key)
        if cache_key not in self._metric_cache:
            metric: Counter = Counter(key, diff=diff)
            self.registry_metric(metric, expire)
        else:
            metric = self._metric_cache.get(cache_key)
        self._set_counter_value(key, value)
        return metric

    def get_counter_value(self, key: str, diff: int = 0) -> float:
        """
        get the value of Counter metric
        Note: It is recommended to call the `get_value` method of `set_counter_value` to return the Metric object

        :param key: Counter key name
        :param diff: collection cycle
            The formula for calculating the statistical time is： max_interval / interval * diff
        """
        assert Counter.gen_metric_cache_name(key) in self._metric_cache, KeyError(key)
        assert diff <= self._max_interval, ValueError(f"diff must <={self._max_interval}")
        cnt, index = self._get_now_info()
        value: float = 0.0
        key = Counter.gen_metric_name(key)
        for i in range(diff):
            bucket: dict = self._bucket_list[index - i]
            if bucket["_metadata_cnt"] < cnt:
                continue
            value += bucket.get(key, 0.0)
        return value

    ################
    # gauge metric #
    ################
    def _set_gauge_value(self, key: str, value: float, is_cover: bool = True) -> None:
        if not is_cover:
            value = self.get_gauge_value(key) + value
        self._metric_cache.add(Gauge.gen_metric_name(key), self._max_interval + 5, value)

    def set_gauge_value(self, key: str, expire: float = -1.0, value: float = 1, is_cover: bool = True) -> Gauge:
        """
        Set the value of Gauge metric

        :param key: Gauge key name
        :param expire: When the Gauge expires from cache, if expire=-1.0 then it will never expire
        :param value: Counter value
        :param is_cover: If the value is True, the value will be overwritten, otherwise the value will be updated
        """
        cache_key: str = Gauge.gen_metric_cache_name(key)
        if cache_key not in self._metric_cache:
            metric: Gauge = Gauge(key)
            self.registry_metric(metric, expire)
        else:
            metric = self._metric_cache.get(cache_key)
        self._set_gauge_value(key, value, is_cover=is_cover)
        return metric

    def get_gauge_value(self, key: str) -> float:
        """
        get the value of Counter metric
        Note: It is recommended to call the `get_value` method of `set_counter_value` to return the Metric object

        :param key: Gauge key name
        """
        cache_key = Gauge.gen_metric_cache_name(key)
        assert cache_key in self._metric_cache, KeyError(cache_key)
        return self._metric_cache.get(Gauge.gen_metric_name(key), 0.0)

    #######################
    # statistics callback #
    #######################
    def _run_callback(self) -> Optional[asyncio.Future]:
        loop: asyncio.AbstractEventLoop = self._loop  # type: ignore

        self._statistics_dict = {}
        for key, metric in self._metric_cache.items():
            if not isinstance(metric, Metric):
                continue
            self._statistics_dict[metric.name] = metric.get_value()

        async def _safe_run_callback(fn: _CallbackFn, dict_param: Dict) -> None:
            await asyncio.sleep(0)
            if asyncio.iscoroutinefunction(fn):
                coro: Any = fn(dict_param)
            else:
                coro = loop.run_in_executor(None, partial(fn, dict_param))
            try:
                await coro
            except Exception as e:
                logger.exception(f"{self.__class__.__name__} run {fn} error: {e}")

        async def _real_run_callback() -> None:
            # statistics callback is serial execution, no need to use lock
            if self._statistics_callback_priority_set:
                await asyncio.gather(
                    *[_safe_run_callback(fn, self._statistics_dict) for fn in self._statistics_callback_priority_set]
                )
            if self._statistics_callback_set:
                # statistics callback dict is read only
                copy_dict: dict = copy.deepcopy(self._statistics_dict)
                await asyncio.gather(*[_safe_run_callback(fn, copy_dict) for fn in self._statistics_callback_set])

        if self._statistics_callback_priority_set or self._statistics_callback_set:
            return asyncio.ensure_future(_real_run_callback())
        else:
            return None

    def _statistics_data(self) -> None:
        if self._is_closed:
            return
        loop: asyncio.AbstractEventLoop = self._loop  # type: ignore
        logger.debug("%s run once at loop time: %s" % (self.__class__.__name__, loop.time()))
        # call callback
        if self._statistics_callback_future:
            if not self._statistics_callback_future.done():
                # The callback is still executing
                if (
                    self._statistics_callback_future_run_timestamp
                    + self._statistics_interval * self._statistics_callback_wait_cnt
                    < self._start_timestamp
                ):
                    # the running time does not exceed the maximum allowable number of cycles
                    loop.call_later(self._statistics_interval, self._statistics_data)
                    return
                elif not self._statistics_callback_future.cancelled():
                    logger.warning("Callback collection execution timeout...cancel")
                    self._statistics_callback_future.cancel()
            else:
                safe_del_future(self._statistics_callback_future)

        self._statistics_callback_future_run_timestamp = time.time()
        self._statistics_callback_future = self._run_callback()
        loop.call_later(self._statistics_interval, self._statistics_data)

    def add_callback(self, fn: _CallbackFn) -> None:
        """
        Add a callback that will be executed during the statistics phase,
        the callback will receive a dict parameter similar to {metric1.name: 0.0, metric2.name: 1.0}.

        Note that this parameter is just a copy
        """
        self._statistics_callback_set.add(fn)

    def remove_callback(self, fn: _CallbackFn) -> None:
        """remove the callback"""
        self._statistics_callback_set.remove(fn)

    def add_priority_callback(self, fn: _CallbackFn) -> None:
        """
        Add a callback that will be executed during the statistics phase,
        the callback will receive a dict parameter similar to {metric1.name: 0.0, metric2.name: 1.0}
        """
        self._statistics_callback_priority_set.add(fn)

    def remove_priority_callback(self, fn: _CallbackFn) -> None:
        """remove the callback"""
        self._statistics_callback_priority_set.remove(fn)

    def close(self) -> None:
        """Cancel the execution of the statistics callback"""
        self._is_closed = True
        if self._statistics_callback_future:
            safe_del_future(self._statistics_callback_future)

    def statistics_data(self) -> None:
        """Execute statistics callback by asyncio.loop"""
        if self._is_closed:
            self._loop = get_event_loop()
            self._statistics_callback_future_run_timestamp = time.time()
            self._is_closed = False
            self._loop.call_later(self._statistics_interval, self._statistics_data)

    @property
    def is_closed(self) -> bool:
        return self._is_closed
