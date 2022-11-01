import asyncio

from rap.server import Server


def run_server() -> None:
    loop = asyncio.new_event_loop()
    rpc_server: Server = Server()
    loop.run_until_complete(rpc_server.create_server())

    loop.run_until_complete(rpc_server.run_forever())


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        format="[%(asctime)s %(levelname)s] %(message)s", datefmt="%y-%m-%d %H:%M:%S", level=logging.DEBUG
    )
    run_server()
