import asyncio

from rap.client import Client
from rap.client.processor import CryptoProcessor

client: Client = Client()
client.load_processor(CryptoProcessor("test", "keyskeyskeyskeys"))


# in register, must use async def...
@client.register()
async def async_sum(a: int, b: int) -> int:
    return 0


async def main() -> None:
    await client.start()
    print(f"async result: {await async_sum(1, 3)}")
    await client.stop()


def run_client() -> None:
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        format="[%(asctime)s %(levelname)s] %(message)s", datefmt="%y-%m-%d %H:%M:%S", level=logging.DEBUG
    )
    run_client()
