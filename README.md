# rap
rap(par[::-1]) is advanced and fast python async rpc

`rap` achieves very fast communication through `msgpack` and `Python asyncio` and multiplexing conn, while supporting high concurrency.
Implement the `protobuf` of `Grpc` through Python functions and TypeHint.

Note: The current `rap` API may change significantly in subsequent versions
> The rap first version feature idea comes from [aiorpc](https://github.com/choleraehyq/aiorpc)

[中文文档](https://github.com/so1n/rap/blob/master/README_CH.md)
# 1.Installation
```Bash
pip install rap
```

# 2.Quick Start

## Server
```Python
import asyncio
from typing import Iterator

from rap.server import Server


def sync_sum(a: int, b: int) -> int:
    return a + b


async def async_sum(a: int, b: int) -> int:
    await asyncio.sleep(1)  #  mock io 
    return a + b


async def async_gen(a: int) -> Iterator[int]:
    for i in range(a):
        yield i


loop = asyncio.new_event_loop()
rpc_server = Server()  # init service

# register func
rpc_server.register(sync_sum)
rpc_server.register(async_sum)
rpc_server.register(async_gen)

# run server
loop.run_until_complete(rpc_server.create_server())

try:
    loop.run_forever()
except KeyboardInterrupt:
    # stop server
    loop.run_until_complete(rpc_server.await_closed())
```

## Client
The client supports to call the service by `raw_call` and `call` methods, but this can not fully use the functions of TypeHint, it is recommended to use `@client.register` to register the function and then call it.

Note: For `rap.client` there is no distinction between `async def` and `def`, but functions registered with `@client.register` can be used directly by the user, so functions decorated with `@client.register` should be similar to:
```Python
async def demo(): pass
```

example: 
```Python
import asyncio
from typing import Iterator

from rap.client import Client

client: "Client" = Client()  # init client


# Declare a function with no function. The function name, function type and return type must be the same as the server side function (async def does not differ from def) 
def sync_sum(a: int, b: int) -> int:
    pass


# The decorated function must be an async def function  
@client.register()
async def sync_sum(a: int, b: int) -> int:
    pass


# The decorated function must be the async def function, because the function is a generator syntax, to `yield` instead of `pass` 
@client.register()
async def async_gen(a: int) -> Iterator:
    yield


async def main():
    await client.connect()
    # Call the call method; read the function name and then call `raw_call`. 
    print(f"call result: {await client.call(sync_sum, 1, 2)}")
    # Basic calls to rap.client 
    print(f"raw call result: {await client.raw_call('sync_sum', 1, 2)}")
    
    # Functions registered through `@client.register` can be used directly 
    # await async_sum(1,3) == await client.raw_call('async_sum', 1, 2)
    # It is recommended to use the @client.register method, which can be used by tools such as IDE to determine whether the parameter type is wrong 
    print(f"decorator result: {await sync_sum(1, 3)}")
    async_gen_result: list = []
    
    # Example of an asynchronous generator, which by default opens or reuses the current session of the rap (about the session will be mentioned below) 
    async for i in async_gen(10):
        async_gen_result.append(i)
    print(f"async gen result:{async_gen_result}")

    
asyncio.run(main())
```
# 3.Function Introduction
## 3.1.Register function
The server side supports `def` and `async def`, if it is a `def` function, it will be run with multiple threads. When registering, the TypeHints of the function's parameters and return value will be checked, and an error will be reported if the type does not match the type specified by json.

The server comes with a registration library. If there are duplicate registrations in the same group, an error will be reported. You can use the `group` parameter to define the group to be registered or redefine the name of the registration with the `name` parameter (you also need to specify the corresponding group when the client calls it).

In addition, you can set `is_private` to True when registering, so that the function can only be called by the local rap.client.
```Python
import asyncio
from typing import Iterator

from rap.server import Server


def demo1(a: int, b: int) -> int:
    return a + b


async def demo2(a: int, b: int) -> int:
    await asyncio.sleep(1)
    return a + b


async def demo_gen(a: int) -> Iterator[int]:
    for i in range(a):
        yield i


server: Server = Server()
server.register(demo1)   # register def func
server.register(demo2)   # register async def func 
server.register(demo_gen)  # register async iterator func
server.register(demo2, name='demo2-alias')   # Register with the value of `name` 
server.register(demo2, group='new-group')    # Register and set the groups to be registered 
server.register(demo2, group='root', is_private=True)  # Register and set the group to be registered, and set it to private 
```
For clients, it is recommended to use `client.register` instead of `client.call`, `client.raw_call`.
`client.register` uses Python syntax to define function names, arguments, parameter types, and return value types, 
It allows the caller to call the function as if it were a normal function, and the function can be checked through tools using the TypeHint feature.
Note: When using `client.register`, be sure to use `async def ... `.
```Python
from typing import Iterator
from rap.client import Client

client: Client = Client()


# register func
@client.register()
async def demo1(a: int, b: int) -> int: pass


# register async iterator fun, replace `pass` with `yield` 
# Since `async for` will make multiple requests to the same conn over time, it will check if the session is enabled and automatically reuse the current session if it is enabled, otherwise it will create a new session and use it. 
@client.register()
async def demo_gen(a: int) -> Iterator: yield 


# Register the general function and set the name to demo2-alias 
@client.register(name='demo2-alias')
async def demo2(a: int, b: int) -> int: pass


# Register the general function and set the group to new-group 
@client.register(group='new-group')
async def demo2(a: int, b: int) -> int: pass
```
## 3.2.session
[example](https://github.com/so1n/rap/tree/master/example/session)

`rap` client support session function, after enabling the session, all requests will only be requested through the current session's conn to the corresponding server, while each request, the session_id in the header will set the current session id, convenient for the server to identify.
`rap` sessions support explicit and implicit settings, each with its own advantages and disadvantages, without mandatory restrictions.
```Python
from rap.client import Client

client = Client()


def sync_sum(a: int, b: int) -> int:
    pass


@client.register()
async def async_sum(a: int, b: int) -> int:
    pass


@client.register()
async def async_gen(a: int):
    yield


async def no_param_run():
    # The rap internal implementation uses the session implicitly via the `contextvar` module 
    print(f"sync result: {await client.call(sync_sum, 1, 2)}")
    print(f"async result: {await async_sum(1, 3)}")

    # The asynchronous generator detects if a session is enabled, and if so, it automatically reuses the current session, otherwise it creates a session 
    async for i in async_gen(10):
        print(f"async gen result:{i}")


async def param_run(session: "Session"):
    # By explicitly passing the session parameters in 
    print(f"sync result: {await client.call(sync_sum, 1, 2, session=session)}")
    print(f"sync result: {await client.raw_call('sync_sum', 1, 2, session=session)}")
    # May be a bit unfriendly 
    print(f"async result: {await async_sum(1, 3, session=session)}")

    # The asynchronous generator detects if a session is enabled, and if so, it automatically reuses the current session, otherwise it creates a session 
    async for i in async_gen(10):
        print(f"async gen result:{i}")


async def execute(session: "Session"):
    # The best way to call a session explicitly, using a method similar to the mysql cursor 
    # execute will automatically recognize the type of call 
    print(f"sync result: {await session.execute(sync_sum, arg_list=[1, 2])}")
    print(f"sync result: {await session.execute('sync_sum', arg_list=[1, 2])}")
    print(f"async result: {await session.execute(async_sum(1, 3))}")

    # The asynchronous generator detects if a session is enabled, and if so, it automatically reuses the current session, otherwise it creates a session 
    async for i in async_gen(10):
        print(f"async gen result:{i}")


async def run_once():
    await client.connect()
    # init session
    async with client.session as s:
        await no_param_run()
        await param_run(s)
        await execute(s)
    await client.wait_close()
```
## 3.3.channel
[example](https://github.com/so1n/rap/tree/master/example/channel)

channel supports client-server interaction in a duplex manner, similar to Http's WebSocket, it should be noted that the channel does not support group settings.

Only `@client.register` is supported on the client side to register the channel function, which is characterized by a single argument of type `Channel`.
The channel will maintain a session and will only communicate with the server via a conn from the time the channel is enabled to the time it is closed.
To avoid the use of 'while True', the channel supports the use of 'async for' syntax and the use of 'while await channel.loop()` syntax instead of 'while True
```Python
from rap.client import Channel, Client
from rap.client.model import Response

client = Client()


@client.register()
async def async_channel(channel: Channel):
    await channel.write("hello")  # send data
    cnt: int = 0
    while await channel.loop(cnt < 3):
        cnt += 1
        print(await channel.read_body())  # read data 
    return


@client.register()
async def echo_body(channel: Channel):
    await channel.write("hi!")
    # Reads data, returns only when data is read, and exits the loop if it receives a signal to close the channel 
    async for body in channel.iter_body():
        print(f"body:{body}")
        await channel.write(body)


@client.register()
async def echo_response(channel: Channel):
    await channel.write("hi!")
    # Read the response data (including header data), and return only if the data is read, or exit the loop if a signal is received to close the channel 
    async for response in channel.iter_response():
        response: Response = response  #  help IDE check type.... 
        print(f"response: {response}")
        await channel.write(response.body)
```
## 3.4.ssl
[example](https://github.com/so1n/rap/tree/master/example/ssl)

Due to the high degree of encapsulation of the `Python asyncio` module, `rap` can be used very easily with ssl
```bash
# Quickly generate ssl.crt and ssl.key 
openssl req -newkey rsa:2048 -nodes -keyout rap_ssl.key -x509 -days 365 -out rap_ssl.crt
```
client.py
```Python
from rap.client import Client

client = Client(ssl_crt_path="./rap_ssl.crt")
```
server.py
```Python
from rap.server import Server

rpc_server = Server(
    ssl_crt_path="./rap_ssl.crt",
    ssl_key_path="./rap_ssl.key",
)
```
## 3.5.event
The server side supports `start_event` and `stop_event` for event handling before start and after shutdown respectively.

```Python
from rap.server import Server


async def mock_start():
    print('start event')


async def mock_stop(): 
    print('stop event')


# example 1
server = Server(start_event_list=[mock_start()], stop_event_list=[mock_stop()])
# example 2
server = Server()
server.load_start_event([mock_start()])
server.load_stop_event([mock_stop()])
```
## 3.6.middleware
`rap` currently supports 2 types of middleware::
- Conn middleware: Used when creating conn, such as limiting the total number of links, etc... 
  reference [block.py](https://github.com/so1n/rap/blob/master/rap/server/middleware/conn/block.py),
  The `dispatch` method will pass in a conn object, and then determine whether to release it according to the rules (return await self.call_next(conn)) or reject it (await conn.close) 
- Message middleware: only supports normal function calls (no support for `Channel`), similar to the use of `starlette` middleware 
  reference [access.py](https://github.com/so1n/rap/blob/master/rap/server/middleware/msg/access.py)
  Message middleware will pass in 4 parameters: request(current request object), call_id(current call id), func(current call function), param(current parameter) and request to return call_id and result(function execution result or exception object) 
  
In addition, the middleware supports `start_event_handle` and `stop_event_handle` methods, which are called when the `Server` starts and shuts down respectively.

example:
```Python
from rap.server import Server
from rap.server.middleware import AccessMsgMiddleware, ConnLimitMiddleware

rpc_server = Server()
rpc_server.load_middleware([ConnLimitMiddleware(), AccessMsgMiddleware()])
```
## 3.7.processor
The `rap` processor is used to handle inbound and outbound traffic, where `process_request` is for inbound traffic and `process_response` is for outbound traffic.

The methods of `rap.client` and `rap.server` processors are basically the same, `rap.server` supports `start_event_handle` and `stop_event_handle` methods, which are called when `Server` starts and shuts down respectively 

[server crypto processor example](https://github.com/so1n/rap/blob/master/rap/server/processor/crypto.py)

[client crypto processor example](https://github.com/so1n/rap/blob/master/rap/client/processor/crypto.py)


client load processor example
```Python
from rap.client import Client
from rap.client.processor import CryptoProcessor

client = Client()
client.load_processor([CryptoProcessor('key_id', 'xxxxxxxxxxxxxxxx')])
```
server load processor example
```Python
from rap.server import Server
from rap.server.processor import CryptoProcessor

server = Server()
server.load_processor([CryptoProcessor({'key_id': 'xxxxxxxxxxxxxxxx'})])
```

# 4.Plugin
rap supports plug-in functionality through `middleware` and `processor`, `middleware` only supports the server side, `processor` supports the client and server side

## 4.1.Encrypted transmission
Encrypted transmission only encrypts the request and response body content, not the header etc. While encrypting, the nonce parameter is added to prevent replay, and the timestamp parameter is added to prevent timeout access.

client example:
```Python
from rap.client import Client
from rap.client.processor import CryptoProcessor

client = Client()
# The first parameter is the id of the secret key, the server determines which secret key is used for the current request by the id of the secret key 
# The second parameter is the key of the secret key, currently only support the length of 16 bits of the secret key 
# timeout: Requests that exceed the timeout value compared to the current timestamp will be discarded
# interval: Clear the nonce interval, the shorter the interval, the more frequent the execution, the greater the useless work, the longer the interval, the more likely to occupy memory, the recommended value is the timeout value is 2 times 
client.load_processor([CryptoProcessor('demo_id', 'xxxxxxxxxxxxxxxx', timeout=60, interval=120)])
```
server example:
```Python
from rap.server import Server
from rap.server.processor import CryptoProcessor


server = Server()
# The first parameter is the secret key key-value pair, key is the secret key id, value is the secret key
# timeout: Requests that exceed the timeout value compared to the current timestamp will be discarded
# nonce_timeout: The expiration time of nonce, the recommended setting is greater than timeout 
server.load_processor([CryptoProcessor({"demo_id": "xxxxxxxxxxxxxxxx"}, timeout=60, nonce_timeout=120)])
```
## 4.2. Limit the maximum number of conn
Server-side use only, you can limit the maximum number of links on the server side, more than the set value will not handle new requests
```Python
from rap.server import Server
from rap.server.middleware import ConnLimitMiddleware, IpMaxConnMiddleware 


server = Server()
server.load_middleware(
    [
        # max_conn: Current maximum number of conn
        # block_timeout: Access ban time after exceeding the maximum number of conn
        ConnLimitMiddleware(max_conn=100, block_time=60),
        # ip_max_conn: Maximum number of conn per ip 
        # timeout: The maximum statistics time for each ip, after the time no new requests come in, the relevant statistics will be cleared
        IpMaxConnMiddleware(ip_max_conn=10, timeout=60),
    ]
)
```
## 4.3.Limit ip access
Support restrict single ip or whole segment ip, support both whitelist and blacklist mode, if whitelist is enabled, blacklist mode is disabled by default
```Python
from rap.server import Server
from rap.server.middleware import IpBlockMiddleware 


server = Server()
# allow_ip_list: whitelist, support network segment ip, if filled with allow_ip_list, black_ip_list will be invalid 
# black_ip_list: blacklist, support network segment ip 
server.load_middleware([IpBlockMiddleware(allow_ip_list=['192.168.0.0/31'], block_ip_list=['192.168.0.2'])])
```
# 5.Advanced Features
**TODO**, This feature is not yet implemented 

# 6.Protocol Design
**TODO**, Document is being edited 

# 7.Introduction to the rap transport
**TODO**, Document is being edited 