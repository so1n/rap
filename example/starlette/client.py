from typing import AsyncIterator
from rap.client import Client
from example.starlette.model import sync_sum, async_gen


client: Client = Client()
client.inject(sync_sum)
client.inject(async_gen)


@client.register(name="sync_sum")
async def new_sync_sum(a: int, b: int) -> int:
    pass


@client.register(name="async_gen")
async def new_async_gen(a: int) -> AsyncIterator[int]:
    yield 0


