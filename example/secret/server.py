import asyncio
import aioredis

from rap.manager.redis_manager import redis_manager
from rap.server import Server
from rap.server.middleware.request.crypto import CryptoMiddleware


async def init_redis():
    conn_pool = await aioredis.create_pool("redis://localhost", minsize=1, maxsize=10, encoding="utf-8")
    redis_manager.init(conn_pool)


async def close_redis():
    await redis_manager.close()


async def async_sum(a: int, b: int) -> int:
    await asyncio.sleep(1)  # mock io time
    return a + b


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        format="[%(asctime)s %(levelname)s] %(message)s", datefmt="%y-%m-%d %H:%M:%S", level=logging.DEBUG
    )

    loop = asyncio.new_event_loop()
    rpc_server = Server(
        connect_call_back=[init_redis()],
        close_call_back=[close_redis()]
    )
    rpc_server.load_middleware([CryptoMiddleware({"test": "keyskeyskeyskeys"})])
    rpc_server.register(async_sum)
    loop.run_until_complete(rpc_server.create_server())

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        loop.run_until_complete(rpc_server.wait_closed())
