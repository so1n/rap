import asyncio

from rap.client import Client, Response
from rap.client.processor import CryptoProcessor
from rap.common.channel import UserChannel

client: Client = Client(
    "example", [{"ip": "localhost", "port": 9000}, {"ip": "localhost", "port": 9001}, {"ip": "localhost", "port": 9002}]
)
client.load_processor([CryptoProcessor("test", "keyskeyskeyskeys")])


@client.register()
async def async_channel(channel: UserChannel) -> None:
    await channel.write("hello")
    cnt: int = 0
    while await channel.loop(cnt < 3):
        cnt += 1
        print(await channel.read_body())
    return


@client.register()
async def echo_body(channel: UserChannel) -> None:
    cnt: int = 0
    await channel.write(f"ping! {cnt}")
    async for body in channel.iter_body():
        print(body)
        cnt += 1
        await channel.write(f"ping! {cnt}")


@client.register()
async def echo_response(channel: UserChannel) -> None:
    await channel.write("hi!")
    async for response in channel.iter():
        response: Response = response  # type:ignore # IDE support
        print(f"response: {response}")
        await channel.write(response.body)


async def run_once() -> None:
    await client.start()
    await echo_body()
    await echo_response()
    await async_channel()
    await client.stop()


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        format="[%(asctime)s %(levelname)s %(filename)s line:%(lineno)d] %(message)s",
        datefmt="%y-%m-%d %H:%M:%S",
        level=logging.DEBUG,
    )

    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_once())
