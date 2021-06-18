import asyncio

from rap.server import Server


def raise_msg_exc(a: int, b: int) -> int:
    return int(1 / 0)


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        format="[%(asctime)s %(levelname)s] %(message)s", datefmt="%y-%m-%d %H:%M:%S", level=logging.INFO
    )

    loop = asyncio.new_event_loop()
    rpc_server = Server("example")
    rpc_server.bind()
    rpc_server.register(raise_msg_exc)
    loop.run_until_complete(rpc_server.run_forever())
