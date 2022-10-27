import asyncio
from typing import Any

import pytest
from aredis import StrictRedis  # type: ignore

from rap.common.exceptions import ServerError, TooManyRequest
from rap.common.utils import constant
from rap.server import Request, Response
from rap.server.plugin.processor import limit
from rap.server.plugin.processor.base import BaseProcessor
from tests.conftest import load_processor

pytestmark = pytest.mark.asyncio


class TestLimit:
    async def test_processor_raise_rap_error(self) -> None:
        class TestProcessor(BaseProcessor):
            async def process_request(self, request: Request) -> Request:
                if request.msg_type == constant.MSG_REQUEST:
                    raise TooManyRequest("test")
                return request

            async def process_response(self, response: Response) -> Response:
                return response

        async with load_processor([TestProcessor()], []):
            with pytest.raises(TooManyRequest) as e:
                from tests.conftest import async_sum

                await async_sum(1, 2)

            exec_msg: str = e.value.args[0]
            assert exec_msg == "test"

    async def test_processor_raise_exc(self) -> None:
        class TestProcessor(BaseProcessor):
            async def process_request(self, request: Request) -> Request:
                if request.msg_type == constant.MSG_REQUEST:
                    raise ValueError("test")
                return request

            async def process_response(self, response: Response) -> Response:
                return response

        async with load_processor([TestProcessor()], []):
            with pytest.raises(ServerError) as e:
                from tests.conftest import async_sum

                await async_sum(1, 2)

            exec_msg: str = e.value.args[0]
            assert exec_msg == "test"

    async def test_limit(self) -> None:
        def match_demo_request(request: Request) -> limit.RULE_FUNC_RETURN_TYPE:
            if request.func_name == "async_sum":
                return request.func_name, False
            else:
                return None, False

        def match_ip_request(request: Request) -> limit.RULE_FUNC_RETURN_TYPE:
            host: str = request.context.conn.peer_tuple[0]
            if host in ("127.0.0.1", "::1"):
                return host + "1", False
            else:
                return None, False

        redis: StrictRedis = StrictRedis.from_url("redis://localhost")
        limit_processor = limit.LimitProcessor(
            limit.backend.RedisTokenBucketBackend(redis),
            [
                (match_demo_request, limit.Rule(second=5, gen_token=1, init_token=1, max_token=10, block_time=5)),
                (match_ip_request, limit.Rule(second=5, gen_token=1, init_token=1, max_token=10, block_time=5)),
            ],
        )
        async with load_processor([limit_processor], []):
            result: Any = limit.backend.RedisTokenBucketBackend(redis).expected_time(
                "test", limit.Rule(second=5, max_token=1, block_time=5)
            )
            if asyncio.iscoroutine(result):
                result = await result  # type: ignore
            assert 0 == result

            from tests.conftest import async_sum

            assert 3 == await async_sum(1, 2)
            with pytest.raises(TooManyRequest):
                assert 3 == await async_sum(1, 2)
            with pytest.raises(TooManyRequest):
                assert 3 == await async_sum(1, 2)

    async def test_limit_by_redis_fixed_window_backend(self) -> None:
        def match_ip_request(request: Request) -> limit.RULE_FUNC_RETURN_TYPE:
            host: str = request.context.conn.peer_tuple[0]
            if host in ("127.0.0.1", "::1"):
                return host + "2", False
            else:
                return None, False

        redis: StrictRedis = StrictRedis.from_url("redis://localhost")
        limit_processor = limit.LimitProcessor(
            limit.backend.RedisFixedWindowBackend(redis),
            [
                (match_ip_request, limit.Rule(second=5, gen_token=1, init_token=1, max_token=10, block_time=5)),
            ],
        )

        async with load_processor([limit_processor], []) as c:
            rap_client, _ = c
            result: Any = limit.backend.RedisFixedWindowBackend(redis).expected_time(
                "test", limit.Rule(second=5, max_token=1, block_time=5)
            )
            if asyncio.iscoroutine(result):
                result = await result  # type: ignore
            assert 0 == result
            assert 3 == await rap_client.invoke_by_name("sync_sum", {"a": 1, "b": 2})
            with pytest.raises(TooManyRequest):
                assert 3 == await rap_client.invoke_by_name("sync_sum", {"a": 1, "b": 2})
            with pytest.raises(TooManyRequest):
                assert 3 == await rap_client.invoke_by_name("sync_sum", {"a": 1, "b": 2})

    async def test_limit_by_redis_cell_backend(self) -> None:
        def match_ip_request(request: Request) -> limit.RULE_FUNC_RETURN_TYPE:
            host: str = request.context.conn.peer_tuple[0]
            if host in ("127.0.0.1", "::1"):
                return host + "3", False
            else:
                return None, False

        redis: StrictRedis = StrictRedis.from_url("redis://localhost")
        limit_processor = limit.LimitProcessor(
            limit.backend.RedisCellBackend(redis),
            [
                (match_ip_request, limit.Rule(second=5, max_token=1, block_time=5)),
            ],
        )
        async with load_processor([limit_processor], []) as c:
            rap_client, _ = c
            result: Any = limit.backend.RedisCellBackend(redis).expected_time(
                "test", limit.Rule(second=5, max_token=1, block_time=5)
            )
            if asyncio.iscoroutine(result):
                result = await result  # type: ignore
            assert 0 == result
            assert 3 == await rap_client.invoke_by_name("sync_sum", {"a": 1, "b": 2})
            with pytest.raises(TooManyRequest):
                assert 3 == await rap_client.invoke_by_name("sync_sum", {"a": 1, "b": 2})
            with pytest.raises(TooManyRequest):
                assert 3 == await rap_client.invoke_by_name("sync_sum", {"a": 1, "b": 2})
