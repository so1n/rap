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


loop = asyncio.get_event_loop()
loop.run_until_complete(client.connect())
loop.run_until_complete(main())
client.close()
