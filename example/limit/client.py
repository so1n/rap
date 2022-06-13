import asyncio
import time
from typing import Callable

from rap.client import Client
from rap.common.channel import UserChannel
from rap.common.exceptions import TooManyRequest

client: Client = Client("example")


# in register, must use async def...
@client.register()
async def demo(a: int, b: int) -> int:
    return 0


@client.register()
async def demo1(a: int, b: int) -> int:
    return 0


@client.register_channel()
async def echo_body(channel: UserChannel) -> None:
    cnt: int = 0
    await channel.write(f"ping! {cnt}")
    async for body in channel.iter_body():
        print(body)
        cnt += 1
        await channel.write(f"ping! {cnt}")


async def retry_handle(func: Callable) -> None:
    for i in range(3):
        try:
            print(await func(i, 0))
            break
        except TooManyRequest as e:
            print(f"recv error: {e}")
            await asyncio.sleep(2)


async def main() -> None:
    s_t = time.time()
    await client.start()
    await echo_body()
    try:
        await echo_body()
    except TooManyRequest as e:
        print(f"call channel error: {e}")

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
