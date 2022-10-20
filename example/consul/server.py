import asyncio

from rap.server import Server
from rap.server.plugin.consul import add_consul_client


async def async_sum(a: int, b: int) -> int:
    await asyncio.sleep(1)  # mock io time
    return a + b


async def main() -> None:
    rpc_server: Server = Server()
    rpc_server.register(async_sum)
    await add_consul_client("example", rpc_server).run_forever()


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        format="[%(asctime)s %(levelname)s] %(message)s", datefmt="%y-%m-%d %H:%M:%S", level=logging.DEBUG
    )

    loop = asyncio.new_event_loop()
    loop.run_until_complete(main())
