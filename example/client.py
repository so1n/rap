import asyncio

from rap.client import Client


client = Client()


@client.register
async def sum_num(a: int, b: int, c: int = 4) -> int:
    pass


loop = asyncio.get_event_loop()
loop.run_until_complete(client.connect())
print('result:', loop.run_until_complete(sum_num(1, 2)))
client.close()
