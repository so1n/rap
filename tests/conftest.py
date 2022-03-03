import asyncio
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, AsyncIterator, List, Optional

import pytest

from rap.client import Client
from rap.client.processor.base import BaseProcessor
from rap.common.channel import UserChannel
from rap.server import Server


class AnyStringWith(str):
    def __eq__(self, other: Any) -> bool:
        return self in other


async def sync_sum(a: int, b: int) -> int:
    return a + b


async def sync_gen(a: int) -> AsyncIterator[int]:
    for i in range(a):
        yield i


async def async_sum(a: int, b: int) -> int:
    await asyncio.sleep(1)  # mock io time
    return a + b


async def async_gen(a: int) -> AsyncIterator[int]:
    await asyncio.sleep(1)  # mock io time
    for i in range(a):
        yield i


async def raise_msg_exc(a: int, b: int) -> int:
    return 0


async def raise_server_not_found_func_exc(a: int) -> None:
    pass


async def sleep(second: int) -> None:
    await asyncio.sleep(second)


##########
# server #
##########
async def _init_server(processor_list: Optional[List] = None) -> Server:
    def error_func() -> float:
        return 1 / 0

    async def test_channel(channel: UserChannel) -> None:
        while await channel.loop():
            if await channel.read_body() == "close":
                return
            await asyncio.sleep(0.1)

    rpc_server = Server("test", ping_sleep_time=1, processor_list=processor_list or [])
    rpc_server.register(sync_sum)
    rpc_server.register(async_sum)
    rpc_server.register(sync_gen)
    rpc_server.register(async_gen)
    rpc_server.register(error_func)
    rpc_server.register(test_channel)
    rpc_server.register(sleep)
    return await rpc_server.create_server()


@pytest.fixture
async def rap_server() -> AsyncGenerator[Server, None]:
    server: Server = await _init_server()
    try:
        yield server
    finally:
        await server.shutdown()


@asynccontextmanager
async def process_server(process_list: List[BaseProcessor]) -> AsyncGenerator[Server, None]:
    server: Server = await _init_server(processor_list=process_list)
    try:
        yield server
    finally:
        await server.shutdown()


##########
# client #
##########
def _inject(client: Client) -> None:
    client.inject(sync_gen)
    client.inject(async_gen)
    client.inject(sync_sum)
    client.inject(async_sum)
    client.inject(raise_server_not_found_func_exc)
    client.inject(raise_msg_exc)
    client.inject(sleep)


def _recovery(client: Client) -> None:
    client.recovery(sync_gen)  # type: ignore
    client.recovery(async_gen)  # type: ignore
    client.recovery(sync_sum)  # type: ignore
    client.recovery(async_sum)  # type: ignore
    client.recovery(raise_server_not_found_func_exc)  # type: ignore
    client.recovery(raise_msg_exc)  # type: ignore
    client.recovery(sleep)  # type: ignore


@pytest.fixture
async def rap_client() -> AsyncGenerator[Client, None]:
    client: Client = Client("test", [{"ip": "localhost", "port": "9000"}])
    _inject(client)
    await client.start()
    try:
        yield client
    finally:
        _recovery(client)
        await client.stop()


@asynccontextmanager
async def process_client(process_list: List[BaseProcessor]) -> AsyncGenerator[Client, None]:
    client: Client = Client("test", [{"ip": "localhost", "port": "9000"}])
    _inject(client)
    client.load_processor(process_list)
    await client.start()
    try:
        yield client
    finally:
        _recovery(client)
        await client.stop()
