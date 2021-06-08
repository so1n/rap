import asyncio

from rap.client import Client

client: Client = Client("example")


async def main() -> None:
    client.add_conn("localhost", 9000)
    await client.start()


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        format="[%(asctime)s %(levelname)s] %(message)s", datefmt="%y-%m-%d %H:%M:%S", level=logging.DEBUG
    )

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.run_forever()
