import asyncio

from aredis import StrictRedis  # type: ignore

from rap.server import Server, context


async def async_sum(a: int, b: int) -> int:
    print(context.request)
    await asyncio.sleep(1)  # mock io time
    return a + b


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        format="[%(asctime)s %(levelname)s] %(message)s", datefmt="%y-%m-%d %H:%M:%S", level=logging.DEBUG
    )

    loop = asyncio.new_event_loop()
    redis: StrictRedis = StrictRedis.from_url("redis://localhost")
    rpc_server: Server = Server("example")
    rpc_server.bind()
    rpc_server.register(async_sum)
    loop.run_until_complete(rpc_server.run_forever())
