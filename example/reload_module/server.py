import asyncio
from typing import Iterator

from rap.server import Server
from rap.middleware.conn.access import AccessConnMiddleware
from rap.middleware.msg.access import AccessMsgMiddleware
from rap.middleware.request.access import AccessMiddleware


def sync_sum(a: int, b: int) -> int:
    return a + b


if __name__ == '__main__':
    import logging
    logging.basicConfig(
        format='[%(asctime)s %(levelname)s] %(message)s',
        datefmt='%y-%m-%d %H:%M:%S',
        level=logging.DEBUG
    )

    loop = asyncio.new_event_loop()
    rpc_server = Server(secret_dict={'test': 'keyskeyskeyskeys'})
    rpc_server.register(sync_sum)
    server = loop.run_until_complete(rpc_server.create_server())

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        server.close()
        loop.run_until_complete(server.wait_closed())
