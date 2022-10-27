import asyncio
from typing import AsyncIterator

from rap.client import Client

client: Client = Client()


def sync_sum(a: int, b: int) -> int:
    return 0


# in register, must use async def...
@client.register()
async def async_sum(a: int, b: int) -> int:
    return 0


# in register, must use async def...
@client.register()
async def async_gen(a: int) -> AsyncIterator[int]:
    yield 0


async def main() -> None:
    await client.start()
    print(f"sync result: {await client.invoke(sync_sum)(1, 2)}")
    print(f"sync result: {await client.invoke_by_name('sync_sum', {'a': 1, 'b': 2})}")

    print(f"async result: {await async_sum(1, 3)}")
    async for i in async_gen(10):
        print(f"async gen result:{i}")


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        format="[%(asctime)s %(levelname)s] %(message)s", datefmt="%y-%m-%d %H:%M:%S", level=logging.DEBUG
    )

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
