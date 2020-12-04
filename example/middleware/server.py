import asyncio
from typing import Iterator

from rap.server import Server
from rap.middleware.conn.access import AccessConnMiddleware
from rap.middleware.msg.access import AccessMsgMiddleware
from rap.middleware.raw_request.access import AccessMiddleware
from rap.middleware.request_dispatch.access import AccessMiddleware as AccessRequestDispatchMiddleware


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
    rpc_server = Server(
        conn_middleware_list=[AccessConnMiddleware()],
        msg_middleware_list=[AccessMsgMiddleware()],
        raw_request_middleware_list=[AccessMiddleware()],
        request_dispatch_middleware_list=[AccessRequestDispatchMiddleware()]
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
