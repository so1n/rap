import asyncio

from rap.client import Client


client: 'Client' = Client()


def sync_sum(a: int, b: int) -> int:
    pass


# in register, must use async def...
@client.register
async def sync_sum(a: int, b: int) -> int:
    pass


# in register, must use async def...
@client.register
async def async_gen(a: int):
    yield


async def main():
    await client.connect()
    # client auto use func name
    print(f"call result: {await client.call(sync_sum, 1, 2)}")
    # call function according to protocol
    print(f"raw call result: {await client.raw_call('sync_sum', 1, 2)}")
    # use decorator, client will auto register `sync_sum` func,
    # when you call the decorated function,
    # the action(await sync_sum(1, 3)) is like await client.raw_call('sync_sum', 1, 3)
    print(f"decorator result: {await sync_sum(1, 3)}")
    async_gen_result: list = []
    async for i in async_gen(10):
        async_gen_result.append(i)
    print(f"async gen result:{async_gen_result}")


if __name__ == '__main__':
    import logging
    logging.basicConfig(
        format='[%(asctime)s %(levelname)s] %(message)s',
        datefmt='%y-%m-%d %H:%M:%S',
        level=logging.DEBUG
    )

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
