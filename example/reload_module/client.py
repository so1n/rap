import asyncio

from rap.client import Client

client = Client()


@client.register()
async def sync_sum(a: int, b: int) -> int:
    pass


async def main() -> None:
    await client.connect()
    print(f"sync result: {await client.call(sync_sum, [1, 2])}")
    print(f"reload :{ await client.raw_call('reload', ['test_module', 'sync_sum'], group='registry')}")
    print(f"sync result: {await client.raw_call('sync_sum', [1, 2])}")


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        format="[%(asctime)s %(levelname)s] %(message)s", datefmt="%y-%m-%d %H:%M:%S", level=logging.DEBUG
    )

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
