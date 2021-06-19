import asyncio

from rap.server import Server


async def async_sum(a: int, b: int) -> int:
    await asyncio.sleep(1)  # mock io time
    return a + b


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        format="[%(asctime)s %(levelname)s] %(message)s", datefmt="%y-%m-%d %H:%M:%S", level=logging.DEBUG
    )

    loop = asyncio.new_event_loop()
    rpc_server: Server = Server(
        "example",
        # enable ssl
        ssl_crt_path="./rap_ssl.crt",
        ssl_key_path="./rap_ssl.key",
    )
    rpc_server.register(async_sum)
    loop.run_until_complete(rpc_server.run_forever())
