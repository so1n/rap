import asyncio

import pytest
from aredis import StrictRedis  # type: ignore

from rap.client import Client
from rap.client.processor.circuit_breaker import FuncCircuitBreakerProcessor, HostCircuitBreakerProcessor
from rap.common.collect_statistics import WindowStatistics
from rap.common.exceptions import ServerError
from rap.server import Server
from tests.conftest import async_sum  # type: ignore

pytestmark = pytest.mark.asyncio


class TestCircuitBreaker:
    async def test_host_circuit_breaker(self, rap_server: Server, rap_client: Client) -> None:
        rap_client.load_processor(
            [
                HostCircuitBreakerProcessor(
                    enable_cnt=1, window_statistics=WindowStatistics(interval=1, callback_interval=1)
                )
            ]
        )
        for _ in range(10):
            try:
                await rap_client.raw_call("not_found_func")
            except Exception:
                pass
        await asyncio.sleep(1)

        with pytest.raises(ServerError) as e:
            await rap_client.raw_call("not_found_func")

        exec_msg: str = e.value.args[0]
        assert exec_msg == "Service Unavailable"

    async def test_func_circuit_breaker(self, rap_server: Server, rap_client: Client) -> None:
        rap_client.load_processor(
            [
                FuncCircuitBreakerProcessor(
                    enable_cnt=1, window_statistics=WindowStatistics(interval=1, callback_interval=1)
                )
            ]
        )
        for _ in range(10):
            try:
                await rap_client.raw_call("not_found_func")
            except Exception:
                pass
        await asyncio.sleep(1)

        with pytest.raises(ServerError) as e:
            await rap_client.raw_call("not_found_func")

        exec_msg: str = e.value.args[0]
        assert exec_msg == "Service's func Unavailable"
