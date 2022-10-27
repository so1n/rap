import asyncio

from rap.client import Client

client: Client = Client()


@client.register()
async def sync_sum(a: int, b: int) -> int:
    return 0


async def main() -> None:
    await client.start()
    print(f"sync result: {await client.invoke(sync_sum)(1, 2)}")
    reload_result = await client.invoke_by_name(
        "reload", {"path": "test_module", "func_str": "sync_sum"}, group="registry"
    )
    print(f"reload :{reload_result}")
    print(f"sync result: {await client.invoke_by_name('sync_sum', {'a': 1, 'b': 2})}")


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        format="[%(asctime)s %(levelname)s] %(message)s", datefmt="%y-%m-%d %H:%M:%S", level=logging.DEBUG
    )

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
