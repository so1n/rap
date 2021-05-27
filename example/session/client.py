import asyncio
import time
from typing import AsyncIterator

from rap.client import Client, Request, Response, Session
from rap.client.processor import BaseProcessor
from rap.common.utils import Constant


class CheckSessionProcessor(BaseProcessor):
    def __init__(self) -> None:
        self.session_id: str = ""

    async def process_request(self, request: Request) -> Request:
        if request.num in (Constant.CHANNEL_REQUEST, Constant.MSG_REQUEST):
            assert self.session_id == request.header["session_id"]
        return request

    async def process_response(self, response: Response) -> Response:
        if response.num in (Constant.CHANNEL_RESPONSE, Constant.MSG_RESPONSE):
            assert self.session_id == response.header["session_id"]
        return response


check_session_processor: CheckSessionProcessor = CheckSessionProcessor()
client = Client()
client.add_conn("localhost", 9000)
client.add_conn("localhost", 9001)
client.add_conn("localhost", 9002)
client.load_processor([check_session_processor])


def sync_sum(a: int, b: int) -> int:
    pass


# in register, must use async def...
@client.register()
async def async_sum(a: int, b: int) -> int:
    pass


# in register, must use async def...
@client.register()
async def async_gen(a: int) -> AsyncIterator[int]:
    yield 0


async def no_param_run() -> None:
    print(f"sync result: {await client.call(sync_sum, [1, 2])}")
    print(f"async result: {await async_sum(1, 3)}")

    # async iterator will create session or reuse session
    async for i in async_gen(10):
        print(f"async gen result:{i}")


async def param_run(session: "Session") -> None:
    print(f"sync result: {await client.call(sync_sum, [1, 2], session=session)}")
    print(f"sync result: {await client.raw_call('sync_sum', [1, 2], session=session)}")
    print(f"async result: {await async_sum(1, 3, session=session)}")

    # async iterator will create session or reuse session
    async for i in async_gen(10):
        print(f"async gen result:{i}")


async def execute(session: "Session") -> None:
    print(f"sync result: {await session.execute(sync_sum, arg_list=[1, 2])}")
    print(f"sync result: {await session.execute('sync_sum', arg_list=[1, 2])}")
    print(f"async result: {await session.execute(async_sum(1, 3))}")

    # async iterator will create session or reuse session
    async for i in async_gen(10):
        print(f"async gen result:{i}")


async def run_once() -> None:
    s_t = time.time()
    await client.start()
    async with client.session as s:
        check_session_processor.session_id = s.id
        await no_param_run()
        await param_run(s)
        await execute(s)
    print(time.time() - s_t)
    await client.stop()


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        format="[%(asctime)s %(levelname)s] %(message)s", datefmt="%y-%m-%d %H:%M:%S", level=logging.DEBUG
    )

    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_once())
