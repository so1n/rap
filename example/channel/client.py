import asyncio

from rap.client import Client
from rap.client.transport import Channel


client = Client()


@client.register
async def async_channel(channel: Channel):
    await channel.write('hello')
    cnt: int = 0
    while await channel.loop(cnt < 3):
        cnt += 1
        print(await channel.read_body())
    return


async def run_once():
    await client.connect()
    await async_channel()
    await client.wait_close()


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        format="[%(asctime)s %(levelname)s] %(message)s", datefmt="%y-%m-%d %H:%M:%S", level=logging.DEBUG
    )

    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_once())
