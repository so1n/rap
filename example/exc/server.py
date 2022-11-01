import asyncio

from rap.server import Server


def raise_msg_exc(a: int, b: int) -> int:
    return int(1 / 0)


def run_server() -> None:
    loop = asyncio.new_event_loop()
    rpc_server = Server()
    rpc_server.register(raise_msg_exc)
    loop.run_until_complete(rpc_server.run_forever())


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        format="[%(asctime)s %(levelname)s] %(message)s", datefmt="%y-%m-%d %H:%M:%S", level=logging.INFO
    )
    run_server()
