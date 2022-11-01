import asyncio
from typing import Any, AsyncIterator

from rap.server import Server, UserChannel


def sync_sum(a: int, b: int) -> int:
    return a + b


async def async_sum(a: int, b: int) -> int:
    await asyncio.sleep(1)  # mock io time
    return a + b


async def async_gen(a: int) -> AsyncIterator[int]:
    for i in range(a):
        yield i


async def async_channel(channel: UserChannel) -> None:
    while await channel.loop():
        body: Any = await channel.read_body()
        await channel.write(body)


def create_server() -> Server:
    rpc_server: Server = Server()
    rpc_server.register(sync_sum)
    rpc_server.register(async_sum)
    rpc_server.register(async_gen)
    rpc_server.register(async_channel)
    return rpc_server


def run_server() -> None:
    loop = asyncio.new_event_loop()
    server: Server = create_server()
    loop.run_until_complete(server.run_forever())


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        format="[%(asctime)s %(levelname)s] %(message)s", datefmt="%y-%m-%d %H:%M:%S", level=logging.DEBUG
    )
    run_server()
