import asyncio
from typing import AsyncGenerator, AsyncIterator, Iterator

import pytest

from rap.client import Client
from rap.server import Server

client: Client = Client(host_list=[
    "localhost:9000",
    "localhost:9001",
    "localhost:9002",
])


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

    rpc_server = Server(host=["localhost:9000", "localhost:9001", "localhost:9002"], ping_sleep_time=1)
    rpc_server.register(_sync_sum, "sync_sum")
    rpc_server.register(_async_sum, "async_sum")
    rpc_server.register(_async_gen, "async_gen")
    rpc_server.register(_sync_gen, "sync_gen")
    server: Server = await rpc_server.create_server()
    yield server
    await rpc_server.await_closed()


@pytest.fixture
async def rap_client() -> AsyncGenerator[Client, None]:
    client.transport._process_request_list = []
    client.transport._process_response_list = []
    await client.connect()
    yield client
    await client.await_close()
