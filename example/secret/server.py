import asyncio

import aioredis

from rap.server import Server
from rap.server.processor import CryptoProcessor


async def async_sum(a: int, b: int) -> int:
    await asyncio.sleep(1)  # mock io time
    return a + b


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        format="[%(asctime)s %(levelname)s] %(message)s", datefmt="%y-%m-%d %H:%M:%S", level=logging.DEBUG
    )

    loop = asyncio.new_event_loop()
    rpc_server = Server(redis_url="redis://localhost")
    rpc_server.load_processor([CryptoProcessor({"test": "keyskeyskeyskeys"})])
    rpc_server.register(async_sum)
    loop.run_until_complete(rpc_server.create_server())

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        loop.run_until_complete(rpc_server.await_closed())
