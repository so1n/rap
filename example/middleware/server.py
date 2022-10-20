import asyncio
from typing import AsyncIterator

from rap.server import Server
from rap.server.plugin.middleware import ConnLimitMiddleware


def sync_sum(a: int, b: int) -> int:
    return a + b


async def async_sum(a: int, b: int) -> int:
    await asyncio.sleep(1)  # mock io time
    return a + b


async def async_gen(a: int) -> AsyncIterator[int]:
    for i in range(a):
        yield i


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        format="[%(asctime)s %(levelname)s] %(message)s", datefmt="%y-%m-%d %H:%M:%S", level=logging.DEBUG
    )

    loop = asyncio.new_event_loop()
    rpc_server = Server()
    rpc_server.load_middleware([ConnLimitMiddleware()])
    rpc_server.register(sync_sum)
    rpc_server.register(async_sum)
    rpc_server.register(async_gen)
    loop.run_until_complete(rpc_server.run_forever())
