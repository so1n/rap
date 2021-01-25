import asyncio

from rap.client import Channel, Client
from rap.client.model import Response
from rap.client.processor import CryptoProcessor

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


@client.register()
async def echo_body(channel: Channel):
    await channel.write("hi!")
    async for body in channel.iter_body():
        print(f"body:{body}")
        await channel.write(body)


@client.register()
async def echo_response(channel: Channel):
    await channel.write("hi!")
    async for response in channel.iter_response():
        response: Response = response  # IDE cannot check
        print(f"response: {response}")
        await channel.write(response.body)


async def run_once():
    await client.connect()
    await echo_body()
    await echo_response()
    await async_channel()
    await client.wait_close()


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        format="[%(asctime)s %(levelname)s] %(message)s", datefmt="%y-%m-%d %H:%M:%S", level=logging.INFO
    )

    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_once())
