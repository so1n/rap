import asyncio
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, AsyncIterator, Iterator, List

import pytest

from rap.client import Client
from rap.client.processor.base import BaseProcessor
from rap.server import Server


class AnyStringWith(str):
    def __eq__(self, other: Any) -> bool:
        return self in other


client: Client = Client("test", [{"ip": "localhost", "port": "9000"}])


def sync_sum(a: int, b: int) -> int:
    pass


@client.register()
async def sync_gen(a: int) -> AsyncIterator[int]:
    yield 0


@client.register()
async def async_sum(a: int, b: int) -> int:
    pass


@client.register()
async def async_gen(a: int) -> AsyncIterator[int]:
    yield 0


@client.register()
async def raise_msg_exc(a: int, b: int) -> int:
    pass


@client.register()
async def raise_server_not_found_func_exc(a: int) -> None:
    pass


@pytest.fixture
async def rap_server() -> AsyncGenerator[Server, None]:
    def _sync_sum(a: int, b: int) -> int:
        return a + b

    def _sync_gen(a: int) -> Iterator[int]:
        for i in range(a):
            yield i

    async def _async_sum(a: int, b: int) -> int:
        await asyncio.sleep(1)  # mock io time
        return a + b

    async def _async_gen(a: int) -> AsyncIterator[int]:
        for i in range(a):
            yield i

    def error_func() -> float:
        return 1 / 0

    rpc_server = Server("test", ping_sleep_time=1)
    rpc_server.register(_sync_sum, "sync_sum")
    rpc_server.register(_async_sum, "async_sum")
    rpc_server.register(_async_gen, "async_gen")
    rpc_server.register(_sync_gen, "sync_gen")
    rpc_server.register(error_func)
    server: Server = await rpc_server.create_server()
    try:
        yield server
    finally:
        await rpc_server.shutdown()


@pytest.fixture
async def rap_client() -> AsyncGenerator[Client, None]:
    client.transport._processor_list = []
    await client.start()
    try:
        yield client
    finally:
        await client.stop()


@asynccontextmanager
async def process_client(process_list: List[BaseProcessor]) -> AsyncGenerator[Client, None]:
    client.transport._processor_list = []
    client.load_processor(process_list)
    await client.start()
    try:
        yield client
    finally:
        await client.stop()
