import time

import pytest
from pytest_mock import MockFixture

from rap.common import cache

pytestmark = pytest.mark.asyncio


class TestCache:
    def test_not_event_loop(self) -> None:
        my_cache: cache.Cache = cache.Cache()
        with pytest.raises(RuntimeError):
            my_cache.add("test", 10)

    async def test_like_set(self) -> None:
        my_cache: cache.Cache = cache.Cache()
        my_cache.add("test", 10)

        assert "test" in my_cache
        my_cache.pop("test")

    async def test_like_dict(self, mocker: MockFixture) -> None:
        mocker.patch("time.time").return_value = time.time()
        key: str = "test"
        value: int = 1
        now_timestamp: float = time.time()
        my_cache: cache.Cache = cache.Cache()
        my_cache.add(key, 10, value)

        assert key in my_cache
        # test get
        assert my_cache.get(key) == value
        assert my_cache.get("error_key", "default") == "default"
        with pytest.raises(KeyError):
            my_cache.get("error_key")
        # test item
        _flag: bool = False
        for _key, _value in my_cache.items():
            if key == _key and _value == value:
                _flag = True
        assert _flag
        # test expire
        assert now_timestamp + 9 <= my_cache._dict[key][0] <= now_timestamp + 10
        my_cache.update_expire(key, 20)
        assert now_timestamp + 19 <= my_cache._dict[key][0] <= now_timestamp + 20

    async def test_expire(self, mocker: MockFixture) -> None:
        mocker.patch("time.time").return_value = 1600000000
        key: str = "test"
        no_expire_key: str = "test_no_expire"
        value: int = 1
        my_cache: cache.Cache = cache.Cache()
        my_cache.add(key, 10, value)
        my_cache.add(no_expire_key, -1, value)
        mocker.patch("time.time").return_value = 1600000011
        my_cache._auto_remove()
        assert my_cache.get(no_expire_key) == value
        with pytest.raises(KeyError):
            my_cache.get(key)
