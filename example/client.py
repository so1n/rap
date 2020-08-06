import asyncio

from rap.client import Client


client = Client()


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


async def main():
    print(f"sync result: {await client.call(sync_sum, 1, 2)}")
    print(f"sync result: {await client.call_by_text('sync_sum', 1, 2)}")
    print(f"async result: {await async_sum(1, 3)}")
    async for i in async_gen(10):
        print(f"async gen result:{i}")


async def main1():
    await asyncio.wait([main() for i in range(100)])

loop = asyncio.get_event_loop()
loop.run_until_complete(client.create_pool())
import time
s = time.time()
loop.run_until_complete(main1())
print(time.time() - s)
client.close()
