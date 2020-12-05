import asyncio
from typing import Iterator

from rap.server import Server
from rap.server.middleware.conn.access import AccessConnMiddleware
from rap.server.middleware.msg import AccessMsgMiddleware
from rap.server.middleware.raw_request import AccessMiddleware
from rap.server.middleware.request_dispatch.access import AccessMiddleware as AccessRequestDispatchMiddleware
from rap.server.middleware.response.print_result import PrintResultMiddleware


def sync_sum(a: int, b: int) -> int:
    return a + b


async def async_sum(a: int, b: int) -> int:
    await asyncio.sleep(1)  # mock io time
    return a + b


async def async_gen(a: int) -> Iterator[int]:
    for i in range(a):
        yield i


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        format="[%(asctime)s %(levelname)s] %(message)s", datefmt="%y-%m-%d %H:%M:%S", level=logging.DEBUG
    )

    loop = asyncio.new_event_loop()
    rpc_server = Server()
    rpc_server.load_middleware(
        [
            AccessConnMiddleware(), AccessMsgMiddleware(), AccessMiddleware(), AccessRequestDispatchMiddleware(),
            PrintResultMiddleware()
        ]
    )
    rpc_server.register(sync_sum)
    rpc_server.register(async_sum)
    rpc_server.register(async_gen)
    server = loop.run_until_complete(rpc_server.create_server())

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        server.close()
        loop.run_until_complete(server.wait_closed())
