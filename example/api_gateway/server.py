import asyncio
from typing import Any, AsyncIterator

from rap.server import Channel, Server


def sync_sum(a: int, b: int) -> int:
    return a + b


async def async_sum(a: int, b: int) -> int:
    await asyncio.sleep(1)  # mock io time
    return a + b


async def async_gen(a: int) -> AsyncIterator[int]:
    for i in range(a):
        yield i


async def async_channel(channel: Channel) -> None:
    while await channel.loop():
        body: Any = await channel.read_body()
        await channel.write(body)


def create_server(server_name: str) -> Server:
    rpc_server: Server = Server(server_name)
    rpc_server.bind()
    rpc_server.register(sync_sum)
    rpc_server.register(async_sum)
    rpc_server.register(async_gen)
    rpc_server.register(async_channel)
    return rpc_server


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        format="[%(asctime)s %(levelname)s] %(message)s", datefmt="%y-%m-%d %H:%M:%S", level=logging.DEBUG
    )

    loop = asyncio.new_event_loop()
    server: Server = create_server("example")
    loop.run_until_complete(server.create_server())

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        loop.run_until_complete(server.await_closed())
