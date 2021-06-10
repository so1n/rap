import asyncio
import time
from typing import Callable

from rap.client import Client
from rap.common.exceptions import TooManyRequest

client: Client = Client("example", [{"ip": "localhost", "port": "9000"}])


# in register, must use async def...
@client.register()
async def demo(a: int, b: int) -> int:
    pass


@client.register()
async def demo1(a: int, b: int) -> int:
    pass


async def retry_handle(func: Callable) -> None:
    for i in range(3):
        while True:
            try:
                print(await func(i, 0))
                break
            except TooManyRequest as e:
                print(f"recv error: {e}")
                print("limiting...sleep 10")
                await asyncio.sleep(10)


async def main() -> None:
    s_t = time.time()
    await client.start()
    await retry_handle(demo)
    await retry_handle(demo1)

    print(time.time() - s_t)
    await client.stop()


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        format="[%(asctime)s %(levelname)s] %(message)s", datefmt="%y-%m-%d %H:%M:%S", level=logging.INFO
    )

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
