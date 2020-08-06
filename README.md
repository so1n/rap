# pait
rap(par[::-1]) is a simple and fast python async rpc

> Refer to the design of [aiorpc](https://github.com/choleraehyq/aiorpc)
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


def async_sum(a: int, b: int):
    return a + b


async def async_gen(a):
    for i in range(a):
        yield i


loop = asyncio.new_event_loop()
rpc_server = Server()
rpc_server.register(sync_sum)
rpc_server.register(async_sum)
rpc_server.register(async_gen)
server = loop.run_until_complete(rpc_server.create_server())

try:
    loop.run_forever()
except KeyboardInterrupt:
    server.close()
    loop.run_until_complete(server.wait_closed())
```

## Client
```Python
import asyncio

from rap.client import Client


client = Client()


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


async def main():
    print(f"sync result: {await client.call(sync_sum, 1, 2)}")
    print(f"sync result: {await client.call_by_text('sync_sum', 1, 2)}")
    
    # client.register
    print(f"async result: {await async_sum(1, 3)}")
    async for i in async_gen(10):
        print(f"async gen result:{i}")


loop = asyncio.get_event_loop()
loop.run_until_complete(client.connect())
loop.run_until_complete(main())
client.close()

```