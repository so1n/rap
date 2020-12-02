import asyncio
from typing import Iterator

from rap.server import Server
from rap.middleware.conn.access import AccessConnMiddleware
from rap.middleware.msg.access import AccessMsgMiddleware
from rap.middleware.request.access import AccessMiddleware


def raise_msg_exc(a: int, b: int) -> int:
    return int(1 / 0)


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        format="[%(asctime)s %(levelname)s] %(message)s", datefmt="%y-%m-%d %H:%M:%S", level=logging.INFO
    )

    loop = asyncio.new_event_loop()
    rpc_server = Server(
        secret_dict={"test": "keyskeyskeyskeys"},
        conn_middleware_list=[AccessConnMiddleware()],
        msg_middleware_list=[AccessMsgMiddleware()],
        request_middleware_list=[AccessMiddleware()],
    )
    rpc_server.register(raise_msg_exc)
    server = loop.run_until_complete(rpc_server.create_server())

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        server.close()
        loop.run_until_complete(server.wait_closed())
