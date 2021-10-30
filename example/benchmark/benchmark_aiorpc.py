"""fork by https://github.com/choleraehyq/aiorpc/blob/master/benchmarks/benchmark_aiorpc_inet.py"""
import asyncio
import multiprocessing
import time

import aiorpc
import uvloop
from aiorpc import server
from aiorpc.constants import MSGPACKRPC_REQUEST
from aiorpc.exceptions import MethodNotFoundError, RPCProtocolError


def _parse_request(req: tuple) -> tuple:
    if len(req) != 4 or req[0] != MSGPACKRPC_REQUEST:
        raise RPCProtocolError("Invalid protocol")

    _, msg_id, method_name, args = req

    method_name = method_name.decode()
    _method_soup = method_name.split(".")
    if len(_method_soup) == 1:
        method = server._methods.get(method_name)
    else:
        method = getattr(server._class_methods.get(_method_soup[0]), _method_soup[1])

    if not method:
        raise MethodNotFoundError("No such method {}".format(method_name))

    return msg_id, method, args, method_name


# fix aiorpc bug
server._parse_request = _parse_request

NUM_CALLS = 10000


def run_sum_server() -> None:
    async def sum(x: int, y: int) -> int:
        # await asyncio.sleep(0.01)
        return x + y

    aiorpc.register("sum", sum)
    loop = uvloop.new_event_loop()
    asyncio.set_event_loop(loop)
    coro = asyncio.start_server(aiorpc.serve, "localhost", 6000, loop=loop)
    loop.run_until_complete(coro)
    loop.run_forever()


def call() -> None:
    async def do(cli: aiorpc.RPCClient) -> None:
        for i in range(NUM_CALLS):
            await cli.call("sum", 1, 2)
            # print('{} call'.format(i))

    client: aiorpc.RPCClient = aiorpc.RPCClient("localhost", 6000)
    loop: asyncio.AbstractEventLoop = uvloop.new_event_loop()
    asyncio.set_event_loop(loop)
    start: float = time.time()

    loop.run_until_complete(do(client))

    print("call: %d qps" % (NUM_CALLS / (time.time() - start)))


if __name__ == "__main__":
    p = multiprocessing.Process(target=run_sum_server)
    p.start()

    time.sleep(1)

    call()

    p.terminate()
