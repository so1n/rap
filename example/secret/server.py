import asyncio

from rap.server import Server
from rap.server.middleware.request_dispatch.crypto import CryptoMiddleware


async def async_sum(a: int, b: int) -> int:
    await asyncio.sleep(1)  # mock io time
    return a + b


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        format="[%(asctime)s %(levelname)s] %(message)s", datefmt="%y-%m-%d %H:%M:%S", level=logging.DEBUG
    )

    loop = asyncio.new_event_loop()
    rpc_server = Server()
    rpc_server.load_middleware([CryptoMiddleware({"test": "keyskeyskeyskeys"})])
    rpc_server.register(async_sum)
    server = loop.run_until_complete(rpc_server.create_server())

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        server.close()
        loop.run_until_complete(server.wait_closed())
