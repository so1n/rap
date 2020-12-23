import asyncio
from typing import Iterator

from rap.server import Server


def sync_sum(a: int, b: int) -> int:
    return a + b


async def async_sum(a: int, b: int) -> int:
    await asyncio.sleep(1)  # mock io time
    return a + b


async def async_gen(a: int) -> Iterator[int]:
    for i in range(a):
        yield i


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        format="[%(asctime)s %(levelname)s] %(message)s", datefmt="%y-%m-%d %H:%M:%S", level=logging.INFO
    )

    loop = asyncio.new_event_loop()
    rpc_server_1 = Server(port=9000)
    rpc_server_1.register(sync_sum)
    rpc_server_1.register(async_sum)
    rpc_server_1.register(async_gen)
    loop.run_until_complete(rpc_server_1.create_server())
    rpc_server_2 = Server(port=9001)
    loop.run_until_complete(rpc_server_2.create_server())
    rpc_server_3 = Server(port=9002)
    loop.run_until_complete(rpc_server_3.create_server())
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        loop.run_until_complete(rpc_server_1.wait_closed())
        loop.run_until_complete(rpc_server_2.wait_closed())
        loop.run_until_complete(rpc_server_3.wait_closed())
