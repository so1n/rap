# rap
rap(par[::-1]) is advanced and fast python async rpc

> Inspired by the sky [aiorpc](https://github.com/choleraehyq/aiorpc)
# Installation
```Bash
pip install rap
```

# Usage

## Server
```Python
import asyncio

from rap.server import Server


def sync_sum(a: int, b: int):
    return a + b


async def mock_io(a: int, b: int):
    await asyncio.sleep(1)  # mock io time
    return a + b


async def async_gen(a):
    for i in range(a):
        yield i


loop = asyncio.new_event_loop()
rpc_server = Server()
rpc_server.register(sync_sum)
rpc_server.register(mock_io)
rpc_server.register(async_gen)
server = loop.run_until_complete(rpc_server.create_server())

try:
    loop.run_forever()
except KeyboardInterrupt:
    server.close()
    loop.run_until_complete(server.wait_closed())
```

## Client
For the client, there is no difference between `async def` and `def`. In fact, `rap` is still called by the method of `call_by_text`. Using `async def` can be decorated by `client.register` and can also be written with the help of IDE Correct code
```Python
import asyncio

from rap.client import Client


client = Client()


def sync_sum(a: int, b: int) -> int:
    pass

# in register, must use async def...
@client.register
async def mock_io(a: int, b: int) -> int:
    pass


# in register, must use async def...
@client.register
async def async_gen(a: int):
    yield


async def main():
    await client.connect()
    print(f"sync result: {await client.call(sync_sum, 1, 2)}")
    print(f"sync result: {await client.raw_call('sync_sum', 1, 2)}")
    print(f"async result: {await mock_io(1, 3)}")
    async for i in async_gen(10):
        print(f"async gen result:{i}")
    await client.wait_close()


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
```