import asyncio
import time

from rap.client import Client


client = Client(secret='keyskeyskeyskeys')


def sync_sum(a: int, b: int) -> int:
    pass


# in register, must use async def...
@client.register
async def async_sum(a: int, b: int) -> int:
    pass


# in register, must use async def...
@client.register
async def async_gen(a: int):
    yield


async def run_once():
    print(f"sync result: {await client.call(sync_sum, 1, 2)}")
    print(f"reload :{ await client.call_by_text('_root_reload', 'test_module', 'sync_sum')}")
    print(f"sync result: {await client.call_by_text('sync_sum', 1, 2)}")
    print(f"sync result: {await client.call_by_text('sync_sum', 1, 2)}")
    print(f"sync result: {await client.call_by_text('sync_sum', 1, 2)}")
    print(f"sync result: {await client.call_by_text('sync_sum', 1, 2)}")

    print(f"async result: {await async_sum(1, 3)}")
    async for i in async_gen(10):
        print(f"async gen result:{i}")


async def conn():
    s_t = time.time()
    await client.connect()
    await asyncio.wait([run_once() for i in range(100)])
    print(time.time() - s_t)
    client.close()


async def pool():
    s_t = time.time()
    await client.create_pool()
    await asyncio.wait([run_once() for i in range(1)])
    print(time.time() - s_t)
    client.close()


if __name__ == '__main__':
    import logging
    logging.basicConfig(
        format='[%(asctime)s %(levelname)s] %(message)s',
        datefmt='%y-%m-%d %H:%M:%S',
        level=logging.DEBUG
    )

    loop = asyncio.get_event_loop()
    loop.run_until_complete(conn())
    loop.run_until_complete(pool())
