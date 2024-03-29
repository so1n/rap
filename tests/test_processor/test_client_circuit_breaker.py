import asyncio
import time
from typing import Optional

import pytest
from aredis import StrictRedis  # type: ignore
from pytest_mock import MockFixture

from rap.client.model import ClientContext, Response
from rap.client.processor.base import BaseClientProcessor, ResponseCallable
from rap.client.processor.circuit_breaker import (
    CircuitBreakerExc,
    FuncCircuitBreakerProcessor,
    HostCircuitBreakerProcessor,
)
from rap.common.collect_statistics import WindowStatistics
from rap.common.exceptions import FuncNotFoundError
from rap.server import Server
from tests.conftest import async_sum, process_client  # type: ignore

pytestmark = pytest.mark.asyncio


class TestCircuitBreaker:
    async def test_host_circuit_breaker(self, rap_server: Server, mocker: MockFixture) -> None:
        now_timestamp: float = time.time()
        circuit_breaker: HostCircuitBreakerProcessor = HostCircuitBreakerProcessor(
            window_statistics=WindowStatistics(interval=1, max_interval=120, statistics_interval=1)
        )
        async with process_client(circuit_breaker) as rap_client:
            for _ in range(10):
                try:
                    await rap_client.invoke_by_name("not_found_func")
                except Exception:
                    pass
            mocker.patch("time.time").return_value = now_timestamp + 2
            await asyncio.sleep(0.1)
            future: Optional[asyncio.Future] = circuit_breaker._window_statistics._run_callback()
            assert future
            await future

            with pytest.raises(CircuitBreakerExc) as e:
                # The circuit breaker mode allows a small number of requests not to be intercepted,
                # and repeating the request multiple times is to increase the probability of being intercepted
                await rap_client.invoke_by_name("not_found_func")
                await rap_client.invoke_by_name("not_found_func")
                await rap_client.invoke_by_name("not_found_func")

            exec_msg: str = e.value.args[0]
            assert exec_msg == "Service Unavailable"

    async def test_func_circuit_breaker(self, rap_server: Server, mocker: MockFixture) -> None:
        now_timestamp: float = time.time()
        circuit_breaker: FuncCircuitBreakerProcessor = FuncCircuitBreakerProcessor(
            window_statistics=WindowStatistics(interval=1, max_interval=120, statistics_interval=1)
        )
        async with process_client(circuit_breaker) as rap_client:
            for _ in range(10):
                try:
                    await rap_client.invoke_by_name("not_found_func")
                except Exception:
                    pass
            mocker.patch("time.time").return_value = now_timestamp + 2
            future: Optional[asyncio.Future] = circuit_breaker._window_statistics._run_callback()
            assert future
            await future

            with pytest.raises(CircuitBreakerExc) as e:
                # The circuit breaker mode allows a small number of requests not to be intercepted,
                # and repeating the request multiple times is to increase the probability of being intercepted
                await rap_client.invoke_by_name("not_found_func")
                await rap_client.invoke_by_name("not_found_func")
                await rap_client.invoke_by_name("not_found_func")

            exec_msg: str = e.value.args[0]
            assert exec_msg == "Service's func Unavailable"

    async def test_circuit_breaker_ignore_processor_exc(self, rap_server: Server, mocker: MockFixture) -> None:
        class MyProcessor(BaseClientProcessor):
            async def on_response(self, response_cb: ResponseCallable, context: ClientContext) -> Response:
                try:
                    return await super().on_response(response_cb, context)
                except Exception as e:
                    response: Response = (await response_cb(False))[0]
                    if response.func_name == "sync_num":
                        raise RuntimeError("test")
                    raise e

        now_timestamp: float = time.time()
        circuit_breaker: FuncCircuitBreakerProcessor = FuncCircuitBreakerProcessor(
            window_statistics=WindowStatistics(interval=1, max_interval=120, statistics_interval=1)
        )
        async with process_client(circuit_breaker, MyProcessor()) as rap_client:
            for _ in range(10):
                try:
                    await rap_client.invoke_by_name("sync_num", {"a": 1, "b": 2})
                except Exception:
                    pass
            mocker.patch("time.time").return_value = now_timestamp + 2
            future: Optional[asyncio.Future] = circuit_breaker._window_statistics._run_callback()
            assert future
            await future

            with pytest.raises(FuncNotFoundError):
                await rap_client.invoke_by_name("not_found_func")
