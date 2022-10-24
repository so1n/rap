import asyncio
from typing import Optional

import pytest
from aredis import StrictRedis  # type: ignore

from rap.client.processor.circuit_breaker import (
    CircuitBreakerExc,
    FuncCircuitBreakerProcessor,
    HostCircuitBreakerProcessor,
)
from rap.common.collect_statistics import WindowStatistics
from rap.server import Server
from tests.conftest import async_sum, process_client  # type: ignore

pytestmark = pytest.mark.asyncio


class TestCircuitBreaker:
    async def test_host_circuit_breaker(self, rap_server: Server) -> None:
        circuit_breaker: HostCircuitBreakerProcessor = HostCircuitBreakerProcessor(
            window_statistics=WindowStatistics(interval=1, max_interval=120, statistics_interval=1)
        )
        async with process_client(circuit_breaker) as rap_client:
            for _ in range(10):
                try:
                    await rap_client.invoke_by_name("not_found_func")
                except Exception:
                    pass
            await asyncio.sleep(1.2)
            future: Optional[asyncio.Future] = circuit_breaker._window_statistics._run_callback()
            if future:
                await future

            with pytest.raises(CircuitBreakerExc) as e:
                await rap_client.invoke_by_name("not_found_func")

            exec_msg: str = e.value.args[0]
            assert exec_msg == "Service Unavailable"

    async def test_func_circuit_breaker(self, rap_server: Server) -> None:
        circuit_breaker: FuncCircuitBreakerProcessor = FuncCircuitBreakerProcessor(
            window_statistics=WindowStatistics(interval=1, max_interval=120, statistics_interval=1)
        )
        async with process_client(circuit_breaker) as rap_client:
            for _ in range(10):
                try:
                    await rap_client.invoke_by_name("not_found_func")
                except Exception:
                    pass
            await asyncio.sleep(1.1)
            future: Optional[asyncio.Future] = circuit_breaker._window_statistics._run_callback()
            if future:
                await future

            with pytest.raises(CircuitBreakerExc) as e:
                await rap_client.invoke_by_name("not_found_func")

            exec_msg: str = e.value.args[0]
            assert exec_msg == "Service's func Unavailable"
