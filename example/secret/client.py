import asyncio

from rap.client import Client
from rap.client.processor.crypto import CryptoProcessor

client = Client()
client.load_middleware([CryptoProcessor("test", "keyskeyskeyskeys")])


# in register, must use async def...
@client.register
async def async_sum(a: int, b: int) -> int:
    pass


async def main():
    await client.connect()
    print(f"async result: {await async_sum(1, 3)}")
    await client.wait_close()


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        format="[%(asctime)s %(levelname)s] %(message)s", datefmt="%y-%m-%d %H:%M:%S", level=logging.DEBUG
    )

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
