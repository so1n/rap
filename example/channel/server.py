import asyncio
from typing import Any

from aredis import StrictRedis  # type: ignore

from rap.server import Channel, Server
from rap.server.model import Response
from rap.server.plugin.processor import CryptoProcessor


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
        await channel.write(body)


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
        format="[%(asctime)s %(levelname)s] %(message)s", datefmt="%y-%m-%d %H:%M:%S", level=logging.INFO
    )

    loop = asyncio.new_event_loop()
    redis: StrictRedis = StrictRedis.from_url("redis://localhost")
    rpc_server = Server(host=["localhost:9000", "localhost:9001", "localhost:9002"])
    rpc_server.load_processor([CryptoProcessor({"test": "keyskeyskeyskeys"}, redis)])
    rpc_server.register(async_channel)
    rpc_server.register(echo_body)
    rpc_server.register(echo_response)

    loop.run_until_complete(rpc_server.create_server())
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        loop.run_until_complete(rpc_server.await_closed())
