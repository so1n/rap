import asyncio

import aioredis

from rap.manager.redis_manager import redis_manager
from rap.server import Channel, Server
from rap.server.model import ResponseModel
from rap.server.processor import CryptoProcessor


async def init_redis():
    conn_pool = await aioredis.create_pool("redis://localhost", minsize=1, maxsize=10, encoding="utf-8")
    redis_manager.init(conn_pool)


async def close_redis():
    await redis_manager.close()


async def async_channel(channel: Channel):
    while await channel.loop():
        body: any = await channel.read_body()
        if body == "hello":
            cnt: int = 0
            await channel.write(f"hello {cnt}")
            while await channel.loop(cnt < 10):
                cnt += 1
                await channel.write(f"hello {cnt}")
        else:
            await channel.write("I don't know")


async def echo_body(channel: Channel):
    cnt: int = 0
    async for body in channel.iter_body():
        await asyncio.sleep(1)
        cnt += 1
        if cnt > 10:
            break
        await channel.write(body)


async def echo_response(channel: Channel):
    cnt: int = 0
    async for response in channel.iter_response():
        response: ResponseModel = response  # IDE cannot check
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
    rpc_server = Server(
        host=["localhost:9000", "localhost:9001", "localhost:9002"],
        start_event_list=[init_redis()],
        stop_event_list=[close_redis()],
    )
    rpc_server.load_processor([CryptoProcessor({"test": "keyskeyskeyskeys"})])
    rpc_server.register(async_channel)
    rpc_server.register(echo_body)
    rpc_server.register(echo_response)

    loop.run_until_complete(rpc_server.create_server())
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        loop.run_until_complete(rpc_server.await_closed())
