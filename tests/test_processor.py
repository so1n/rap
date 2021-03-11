import pytest

from aredis import StrictRedis
from rap.client import Client
from rap.common.exceptions import TooManyRequest
from rap.server import Server, RequestModel
from rap.server.processor import limit

from .conftest import async_sum


pytestmark = pytest.mark.asyncio


class TestLimit:
    async def test_limit(self, rap_server: Server, rap_client: Client) -> None:
        def match_demo_request(request: RequestModel) -> limit.RULE_FUNC_RETURN_TYPE:
            if request.func_name == "async_sum":
                return request.func_name
            else:
                return None

        def match_ip_request(request: RequestModel) -> limit.RULE_FUNC_RETURN_TYPE:
            key: str = "127.0.0.1"
            if request.header["host"][0] == key:
                return key + "1"
            else:
                return None

        redis: StrictRedis = StrictRedis.from_url("redis://localhost")
        limit_processor = limit.LimitProcessor(
            limit.backend.RedisTokenBucketBackend(redis),
            [
                (match_demo_request, limit.Rule(second=5, gen_token=1, init_token=1, max_token=10, block_time=5)),
                (match_ip_request, limit.Rule(second=5, gen_token=1, init_token=1, max_token=10, block_time=5)),
            ],
        )
        rap_server.load_processor([limit_processor])

        assert 0 == await limit.backend.RedisTokenBucketBackend(redis).expected_time(
            'test', limit.Rule(second=5, max_token=1, block_time=5)
        )

        assert 3 == await async_sum(1, 2)
        with pytest.raises(TooManyRequest):
            assert 3 == await async_sum(1, 2)
        with pytest.raises(TooManyRequest):
            assert 3 == await async_sum(1, 2)

    async def test_limit_by_redis_fixed_window_backend(self, rap_server: Server, rap_client: Client) -> None:
        def match_ip_request(request: RequestModel) -> limit.RULE_FUNC_RETURN_TYPE:
            key: str = "127.0.0.1"
            if request.header["host"][0] == key:
                return key + "2"
            else:
                return None

        redis: StrictRedis = StrictRedis.from_url("redis://localhost")
        limit_processor = limit.LimitProcessor(
            limit.backend.RedisFixedWindowBackend(redis),
            [
                (match_ip_request, limit.Rule(second=5, gen_token=1, init_token=1, max_token=10, block_time=5)),
            ],
        )
        rap_server.load_processor([limit_processor])

        assert 0 == await limit.backend.RedisFixedWindowBackend(redis).expected_time(
            'test', limit.Rule(second=5, max_token=1, block_time=5)
        )
        assert 3 == await rap_client.raw_call('sync_sum', 1, 2)
        with pytest.raises(TooManyRequest):
            assert 3 == await rap_client.raw_call('sync_sum', 1, 2)
        with pytest.raises(TooManyRequest):
            assert 3 == await rap_client.raw_call('sync_sum', 1, 2)

    async def test_limit_by_redis_cell_backend(self, rap_server: Server, rap_client: Client) -> None:
        def match_ip_request(request: RequestModel) -> limit.RULE_FUNC_RETURN_TYPE:
            key: str = "127.0.0.1"
            if request.header["host"][0] == key:
                return key + "3"
            else:
                return None

        redis: StrictRedis = StrictRedis.from_url("redis://localhost")
        limit_processor = limit.LimitProcessor(
            limit.backend.RedisCellBackend(redis),
            [
                (match_ip_request, limit.Rule(second=5, max_token=1, block_time=5)),
            ],
        )
        rap_server.load_processor([limit_processor])

        assert 0 == await limit.backend.RedisCellBackend(redis).expected_time(
            'test', limit.Rule(second=5, max_token=1, block_time=5)
        )
        assert 3 == await rap_client.raw_call('sync_sum', 1, 2)
        with pytest.raises(TooManyRequest):
            assert 3 == await rap_client.raw_call('sync_sum', 1, 2)
        with pytest.raises(TooManyRequest):
            assert 3 == await rap_client.raw_call('sync_sum', 1, 2)
