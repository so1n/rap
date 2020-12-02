import asyncio

from rap.client import Client


client = Client(secret_tuple=("test", "keyskeyskeyskeys"), ssl_crt_path="./rap_ssl.crt")  # enable secret


# in register, must use async def...
@client.register
async def async_sum(a: int, b: int) -> int:
    pass


async def main():
    """
    # gen ssl key sh
    > openssl req -newkey rsa:2048 -nodes -keyout rap_ssl.key -x509 -days 365 -out rap_ssl.crt
    """
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
