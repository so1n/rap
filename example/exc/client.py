import asyncio
import time

from rap.client import Client
from rap.common.exceptions import FuncNotFoundError

client = Client()


# in register, must use async def...
@client.register()
async def raise_msg_exc(a: int, b: int) -> int:
    pass


# in register, must use async def...
@client.register()
async def raise_server_not_found_func_exc(a: int) -> None:
    pass


async def main() -> None:
    s_t = time.time()
    client.add_conn("localhost", 9000)
    await client.start()
    try:
        await raise_msg_exc(1, 2)
    except Exception as e:
        assert isinstance(e, ZeroDivisionError)
    try:
        await raise_server_not_found_func_exc(1)
    except Exception as e:
        assert isinstance(e, FuncNotFoundError)
    print(time.time() - s_t)
    await client.stop()


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        format="[%(asctime)s %(levelname)s] %(message)s", datefmt="%y-%m-%d %H:%M:%S", level=logging.INFO
    )

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
