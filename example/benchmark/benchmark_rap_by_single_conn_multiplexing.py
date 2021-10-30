import asyncio
import multiprocessing
import time
from typing import Coroutine, List

import uvloop

from rap.client import Client
from rap.server import Server

NUM_CALLS: int = 10000


def run_server() -> None:
    async def test_sum(a: int, b: int) -> int:
        await asyncio.sleep(0.01)
        return a + b

    loop: asyncio.AbstractEventLoop = uvloop.new_event_loop()
    asyncio.set_event_loop(loop)
    rpc_server: Server = Server("example")
    rpc_server.register(test_sum)
    loop.run_until_complete(rpc_server.run_forever())


def run_client() -> None:
    loop: asyncio.AbstractEventLoop = uvloop.new_event_loop()
    asyncio.set_event_loop(loop)
    client: Client = Client("example", [{"ip": "localhost", "port": "9000"}])

    @client.register()
    async def test_sum(a: int, b: int) -> int:
        return a + b

    async def request() -> None:
        task_list: List[Coroutine] = [test_sum(1, 2) for _ in range(NUM_CALLS)]
        await asyncio.gather(*task_list)

    loop.run_until_complete(client.start())
    start: float = time.time()
    loop.run_until_complete(request())
    print("call: %d qps" % (NUM_CALLS / (time.time() - start)))
    loop.run_until_complete(client.stop())


if __name__ == "__main__":
    p = multiprocessing.Process(target=run_server)
    p.start()
    time.sleep(1)
    run_client()
    p.terminate()
