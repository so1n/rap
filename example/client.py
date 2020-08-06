import asyncio

from rap.client import Client


async def do(cli):
    ret = await cli.call('sum_num', 1, 1)
    print(f"result: {ret}")


loop = asyncio.get_event_loop()
client = Client()
loop.run_until_complete(client.connect())
loop.run_until_complete(do(client))
client.close()
