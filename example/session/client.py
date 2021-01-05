import asyncio
import time

from rap.client import Client
from rap.common.conn import Connection

client = Client(
    host_list=[
        "localhost:9000",
        "localhost:9001",
        "localhost:9002",
    ]
)


def sync_sum(a: int, b: int) -> int:
    pass


# in register, must use async def...
@client.register
async def async_sum(a: int, b: int) -> int:
    pass


# in register, must use async def...
@client.register
async def async_gen(a: int):
    yield


async def no_conn_param_run():
    print(f"sync result: {await client.call(sync_sum, 1, 2)}")
    print(f"sync result: {await client.raw_call('sync_sum', 1, 2)}")
    print(f"async result: {await async_sum(1, 3)}")

    # async iterator will reuse session
    async for i in async_gen(10):
        print(f"async gen result:{i}")


async def conn_param_run(conn: Connection):
    print(f"sync result: {await client.call(sync_sum, 1, 2, conn=conn)}")
    print(f"sync result: {await client.raw_call('sync_sum', 1, 2, conn=conn)}")
    print(f"async result: {await async_sum(1, 3, conn=conn)}")

    # async iterator will create session
    async for i in async_gen(10):
        print(f"async gen result:{i}")


async def run_once():
    s_t = time.time()
    await client.connect()
    async with client.transport.session as s:
        await no_conn_param_run()
        await conn_param_run(s.conn)
    print(time.time() - s_t)
    await client.wait_close()


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        format="[%(asctime)s %(levelname)s] %(message)s", datefmt="%y-%m-%d %H:%M:%S", level=logging.DEBUG
    )

    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_once())
