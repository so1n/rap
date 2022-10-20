import asyncio

from aredis import StrictRedis  # type: ignore

from rap.server import ReadChannel, Server, UserChannel, WriteChannel
from rap.server.plugin.processor import CryptoProcessor


async def read_channel(channel: WriteChannel) -> None:
    for i in range(10):
        await channel.write(f"hello {i}")


async def write_channel(channel: ReadChannel) -> None:
    async for i in channel:
        print(i)


async def async_channel(channel: UserChannel) -> None:
    while await channel.loop():
        resp = await channel.read()
        if resp.body == "hello":
            cnt: int = 0
            await channel.write(f"hello {cnt}")
            while await channel.loop(cnt < 10):
                cnt += 1
                await channel.write(f"hello {cnt}")
        else:
            await channel.write("I don't know")


async def echo_body(channel: UserChannel) -> None:
    cnt: int = 0
    async for body in channel.iter_body():
        await asyncio.sleep(0.1)
        cnt += 1
        print(cnt, body)
        if cnt > 10:
            break
        await channel.write(f"pong! {cnt}")


async def echo_response(channel: UserChannel) -> None:
    cnt: int = 0
    async for response in channel.iter():
        await asyncio.sleep(0.1)
        cnt += 1
        if cnt > 10:
            break
        await channel.write(response.body)


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        format="[%(asctime)s %(levelname)s %(filename)s line:%(lineno)d] %(message)s",
        datefmt="%y-%m-%d %H:%M:%S",
        level=logging.DEBUG,
    )

    loop = asyncio.new_event_loop()
    rpc_server_1: Server = Server()
    rpc_server_1.load_processor([CryptoProcessor({"test": "keyskeyskeyskeys"})])
    rpc_server_1.register(async_channel)
    rpc_server_1.register(echo_body)
    rpc_server_1.register(echo_response)
    rpc_server_1.register(read_channel)
    rpc_server_1.register(write_channel)

    rpc_server_2: Server = Server(port=9001)
    rpc_server_2.load_processor([CryptoProcessor({"test": "keyskeyskeyskeys"})])
    rpc_server_2.register(async_channel)
    rpc_server_2.register(echo_body)
    rpc_server_2.register(echo_response)
    rpc_server_2.register(read_channel)
    rpc_server_2.register(write_channel)

    rpc_server_3: Server = Server(port=9002)
    rpc_server_3.load_processor([CryptoProcessor({"test": "keyskeyskeyskeys"})])
    rpc_server_3.register(async_channel)
    rpc_server_3.register(echo_body)
    rpc_server_3.register(echo_response)
    rpc_server_3.register(read_channel)
    rpc_server_3.register(write_channel)

    async def run_forever() -> None:
        await asyncio.gather(*[rpc_server_1.run_forever(), rpc_server_2.run_forever(), rpc_server_3.run_forever()])

    loop.run_until_complete(run_forever())
