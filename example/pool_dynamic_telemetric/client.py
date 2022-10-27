import asyncio
import time
from typing import AsyncIterator

from rap.client import Client

client: Client = Client()


async def async_sum(a: int, b: int) -> int:
    return 0


# in register, must use async def...
@client.register(name="async_sum")
async def async_sum_(a: int, b: int) -> int:
    return 0


async def async_gen(a: int) -> AsyncIterator[int]:
    yield 0


# in register, must use async def...
@client.register(name="async_gen")
async def async_gen_(a: int) -> AsyncIterator[int]:
    yield 0


async def _run_once() -> None:
    try:
        await client.invoke(async_sum)(1, 2)
        await client.invoke_by_name("sync_sum", {"a": 1, "b": 2})
        await async_sum_(1, 3)
        # async for i in client.invoke_iterator(async_gen)(3):
        #     pass
        # async for i in async_gen_(3):
        #     pass
    except Exception as e:
        logging.error(f"ignore error:{e}")


async def run() -> None:
    s_t = time.time()
    await asyncio.gather(*[_run_once() for _ in range(10000)])
    print(time.time() - s_t)


async def monitor():
    while True:
        await asyncio.sleep(1)
        print(len(asyncio.all_tasks()))


async def main() -> None:
    await client.start()
    monitor_future: asyncio.Future = asyncio.ensure_future(monitor())
    for i in range(20):
        await run()
        if i == 10:
            print("wait...........")
            await asyncio.sleep(10)
        print(f"..............{i}...........")
    await client.stop()
    monitor_future.cancel()


if __name__ == "__main__":
    import logging

    #
    # logging.basicConfig(
    #     format="[%(asctime)s %(levelname)s] %(message)s", datefmt="%y-%m-%d %H:%M:%S", level=logging.INFO
    # )

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
