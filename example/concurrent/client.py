import asyncio
import time
from typing import AsyncIterator

from rap.client import Client

client: Client = Client("example", [{"ip": "localhost", "port": "9000"}])


def sync_sum(a: int, b: int) -> int:
    pass


# in register, must use async def...
@client.register()
async def async_sum(a: int, b: int) -> int:
    pass


# in register, must use async def...
@client.register()
async def async_gen(a: int) -> AsyncIterator[int]:
    yield 0


async def _run_once() -> None:
    print(f"sync result: {await client.invoke(sync_sum)(1, 2)}")
    # print(f"reload :{ await client.raw_invoke('_root_reload', 'test_module', 'sync_sum')}")
    print(f"sync result: {await client.raw_invoke('sync_sum', [1, 2])}")
    print(f"async result: {await async_sum(1, 3)}")
    async for i in async_gen(10):
        print(f"async gen result:{i}")


async def run_once() -> None:
    s_t = time.time()
    await _run_once()
    print(time.time() - s_t)


async def run_mutli() -> None:
    s_t = time.time()
    await asyncio.wait([_run_once() for _ in range(100)])
    print(time.time() - s_t)


async def main() -> None:
    await client.start()
    await run_once()
    await run_mutli()
    await client.stop()


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        format="[%(asctime)s %(levelname)s] %(message)s", datefmt="%y-%m-%d %H:%M:%S", level=logging.DEBUG
    )

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
