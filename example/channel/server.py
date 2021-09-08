import asyncio
from typing import Any

from aredis import StrictRedis  # type: ignore

from rap.server.core import Server
from rap.server.model import Response
from rap.server.plugin.processor import CryptoProcessor
from rap.server.receiver import Channel


async def async_channel(channel: Channel) -> None:
    while await channel.loop():
        body: Any = await channel.read_body()
        if body == "hello":
            cnt: int = 0
            await channel.write(f"hello {cnt}")
            while await channel.loop(cnt < 10):
                cnt += 1
                await channel.write(f"hello {cnt}")
        else:
            await channel.write("I don't know")


async def echo_body(channel: Channel) -> None:
    cnt: int = 0
    async for body in channel.iter_body():
        await asyncio.sleep(1)
        cnt += 1
        if cnt > 10:
            break
        await channel.write(f"pong! {cnt}")


async def echo_response(channel: Channel) -> None:
    cnt: int = 0
    async for response in channel.iter_response():
        response: Response = response  # type: ignore  # IDE cannot check
        await asyncio.sleep(1)
        cnt += 1
        if cnt > 10:
            break
        await channel.write(response.body)


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        format="[%(asctime)s %(levelname)s] %(message)s", datefmt="%y-%m-%d %H:%M:%S", level=logging.DEBUG
    )

    loop = asyncio.new_event_loop()
    rpc_server_1: Server = Server("example")
    rpc_server_1.load_processor([CryptoProcessor({"test": "keyskeyskeyskeys"})])
    rpc_server_1.register(async_channel)
    rpc_server_1.register(echo_body)
    rpc_server_1.register(echo_response)

    rpc_server_2: Server = Server("example", port=9001)
    rpc_server_2.load_processor([CryptoProcessor({"test": "keyskeyskeyskeys"})])
    rpc_server_2.register(async_channel)
    rpc_server_2.register(echo_body)
    rpc_server_2.register(echo_response)

    rpc_server_3: Server = Server("example", port=9002)
    rpc_server_3.load_processor([CryptoProcessor({"test": "keyskeyskeyskeys"})])
    rpc_server_3.register(async_channel)
    rpc_server_3.register(echo_body)
    rpc_server_3.register(echo_response)

    async def run_forever() -> None:
        await asyncio.gather(*[rpc_server_1.run_forever(), rpc_server_2.run_forever(), rpc_server_3.run_forever()])

    loop.run_until_complete(run_forever())
