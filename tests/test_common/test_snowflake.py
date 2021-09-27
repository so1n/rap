import time

import pytest
from pytest_mock import MockFixture

from rap.common.snowflake import WaitNextSequenceExc, _cache, async_get_snowflake_id, get_snowflake_id

pytestmark = pytest.mark.asyncio


class TestSnowFlake:
    def test_get_snowflake_id(self, mocker: MockFixture) -> None:
        # local host
        _cache.clear()
        for _ in range(10):
            get_snowflake_id()
        assert len(_cache) == 1

        # two local host
        _cache.clear()
        mocker.patch("socket.gethostname").return_value = "test_a"
        for _ in range(10):
            get_snowflake_id()
        mocker.patch("socket.gethostname").return_value = "test_b"
        for _ in range(10):
            get_snowflake_id()
        assert len(_cache) == 2

        # test wait sequence
        index: int = 0
        mocker.patch("time.time").return_value = time.time() + 10
        with pytest.raises(WaitNextSequenceExc):
            for index in range(4096):
                get_snowflake_id(wait_sequence=False)
        assert index == 4095

    async def test_async_get_snowflake_id(self, mocker: MockFixture) -> None:
        # local host
        _cache.clear()
        for _ in range(10):
            await async_get_snowflake_id()
        assert len(_cache) == 1

        # two local host
        _cache.clear()
        mocker.patch("socket.gethostname").return_value = "test_a"
        for _ in range(10):
            await async_get_snowflake_id()
        mocker.patch("socket.gethostname").return_value = "test_b"
        for _ in range(10):
            await async_get_snowflake_id()
        assert len(_cache) == 2

        # test wait sequence
        index: int = 0
        mocker.patch("time.time").return_value = time.time() + 10
        with pytest.raises(WaitNextSequenceExc):
            for index in range(4096):
                await async_get_snowflake_id(wait_sequence=False)
        assert index == 4095
