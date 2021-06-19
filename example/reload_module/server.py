import asyncio

from rap.server import Server


def sync_sum(a: int, b: int) -> int:
    return a + b


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        format="[%(asctime)s %(levelname)s] %(message)s", datefmt="%y-%m-%d %H:%M:%S", level=logging.DEBUG
    )

    loop = asyncio.new_event_loop()
    rpc_server: Server = Server("example")
    rpc_server.register(sync_sum)
    loop.run_until_complete(rpc_server.run_forever())
