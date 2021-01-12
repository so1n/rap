import asyncio

from rap.client import Client
from rap.client.processor.crypto import CryptoProcessor
from rap.client.transoprt.channel import Channel

client = Client(
    host_list=[
        "localhost:9000",
        "localhost:9001",
        "localhost:9002",
    ]
)
client.load_processor([CryptoProcessor("test", "keyskeyskeyskeys")])


@client.register
async def async_channel(channel: Channel):
    await channel.write("hello")
    cnt: int = 0
    while await channel.loop(cnt < 3):
        cnt += 1
        print(await channel.read_body())
    return


@client.register
async def echo(channel: Channel):
    await channel.write("hi!")
    async for body in channel:
        print(body)
        await channel.write(body)


async def run_once():
    await client.connect()
    await echo()
    await async_channel()
    await client.wait_close()


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        format="[%(asctime)s %(levelname)s] %(message)s", datefmt="%y-%m-%d %H:%M:%S", level=logging.INFO
    )

    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_once())
