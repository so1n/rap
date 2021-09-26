import asyncio

import pytest
from pytest_mock import MockFixture

from rap.common.asyncio_helper import get_event_loop
from rap.common.collect_statistics import Counter, Gauge, WindowStatistics

pytestmark = pytest.mark.asyncio


class TestCollectStatistics:
    async def test_gauge(self, mocker: MockFixture) -> None:
        mocker.patch("time.time").return_value = 1600000000
        window_statistics: WindowStatistics = WindowStatistics()
        test_gauge: Gauge = Gauge("test", diff=60)
        window_statistics.registry_metric(test_gauge)
        # test name
        assert test_gauge.metric_cache_name == test_gauge.gen_metric_cache_name(test_gauge.raw_name)
        assert test_gauge.name == test_gauge.gen_metric_name(test_gauge.raw_name)
        # test value
        test_gauge.set_value(10)
        mocker.patch("time.time").return_value = 1600000002
        test_gauge.increment(1)
        mocker.patch("time.time").return_value = 1600000004
        test_gauge.decrement(5)
        assert 11 == test_gauge.get_value()
        mocker.patch("time.time").return_value = 1600000006
        assert 6 == test_gauge.get_value()
        # test statistics value
        assert 0 == test_gauge.get_statistics_value()
        window_statistics._is_closed = False
        window_statistics._loop = get_event_loop()
        window_statistics._statistics_data()
        await asyncio.sleep(0.1)
        assert 6 == test_gauge.get_statistics_value()
        # test window side
        mocker.patch("time.time").return_value = 1600000062
        assert -4 == test_gauge.get_value()
        mocker.patch("time.time").return_value = 1600000070
        assert 0 == test_gauge.get_value()

    async def test_counter(self, mocker: MockFixture) -> None:
        mocker.patch("time.time").return_value = 1600000000
        window_statistics: WindowStatistics = WindowStatistics()
        test_counter: Counter = Counter("test")
        window_statistics.registry_metric(test_counter, expire=60)
        # test name
        assert test_counter.metric_cache_name == test_counter.gen_metric_cache_name(test_counter.raw_name)
        assert test_counter.name == test_counter.gen_metric_name(test_counter.raw_name)
        # test value
        test_counter.set_value(10)
        mocker.patch("time.time").return_value = 1600000002
        test_counter.set_value(1)
        mocker.patch("time.time").return_value = 1600000004
        test_counter.set_value(5)
        assert 5 == test_counter.get_value()
        # test statistics value
        assert 0 == test_counter.get_statistics_value()
        window_statistics._is_closed = False
        window_statistics._loop = get_event_loop()
        window_statistics._statistics_data()
        await asyncio.sleep(0.1)
        assert 5 == test_counter.get_statistics_value()
        # test windows side
        mocker.patch("time.time").return_value = 1600000070
        window_statistics._metric_cache._auto_remove()
        with pytest.raises(AssertionError):
            assert 5 == test_counter.get_value()
        # key not exist, but statistics only change by `window_statistics._statistics_data()`
        assert 5 == test_counter.get_statistics_value()
        window_statistics._statistics_data()
        assert 0 == test_counter.get_statistics_value()

    async def test_callback(self, mocker: MockFixture) -> None:
        mocker.patch("time.time").return_value = 1600000000
        window_statistics: WindowStatistics = WindowStatistics()
        window_statistics.set_counter_value("test_counter", 60, 10)
        window_statistics.set_gauge_value("test_gauge", 60, 10, 10)
        assert_dict: dict = {}

        def _callback(_dict: dict) -> None:
            assert_dict.update(_dict)

        def _priority_callback(_dict: dict) -> None:
            _dict["new_value"] = 30

        window_statistics.add_callback(_callback)
        window_statistics.add_priority_callback(_priority_callback)

        mocker.patch("time.time").return_value = 1600000008
        window_statistics._is_closed = False
        window_statistics._loop = get_event_loop()
        window_statistics._statistics_data()
        await asyncio.sleep(0.1)
        assert assert_dict["new_value"] == 30
        assert assert_dict["counter_test_counter"] == 10
        assert assert_dict["gauge_test_gauge"] == 10
