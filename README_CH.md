# rap
rap(par[::-1]) 是一个速度快且支持高级功能的rpc框架 

`rap`依赖于`msgpack`和`Python asyncio`以及本身实现的链接复用使得传输速度非常快,同时支持高并发.通过Python的函数和TypeHint实现类似于`Grpc`的`protobuf`.

注意: 目前`rap`的后续版本API变动可能较大
> rap第一版功能思路来自于 [aiorpc](https://github.com/choleraehyq/aiorpc)
# 1.安装 
```Bash
pip install rap
```

# 2.快速上手 

## 服务端 
```Python
import asyncio
from typing import Iterator

from rap.server import Server


def sync_sum(a: int, b: int) -> int:
    return a + b


async def async_sum(a: int, b: int) -> int:
    await asyncio.sleep(1)  #  模拟io处理 
    return a + b


async def async_gen(a: int) -> Iterator[int]:
    for i in range(a):
        yield i


loop = asyncio.new_event_loop()
rpc_server = Server()  # 初始化服务

# 注册处理函数
rpc_server.register(sync_sum)
rpc_server.register(async_sum)
rpc_server.register(async_gen)
# 运行服务
loop.run_until_complete(rpc_server.create_server())

try:
    loop.run_forever()
except KeyboardInterrupt:
    # 关闭服务
    loop.run_until_complete(rpc_server.await_closed())
```

## 客户端
客户端支持通过`raw_call`和`call`方法来调用服务,但这样无法完整利用到TypeHint的功能,推荐使用`@client.register`注册函数再进行调用
注意: 对于`rap.client`来说并不会区分`async def`和`def`, 但是使用`@client.register`注册的函数可以被用户直接使用,所以被`@client.register`装饰的函数要类似于:
```Python
async def demo(): pass
```

快速上手例子:
```Python
import asyncio

from rap.client import Client

client: "Client" = Client()  # 初始化客户端


# 声明一个无功能的函数,函数,函数类型和返回类型必须与服务端的函数一致(async def 与def并无差别)
def sync_sum(a: int, b: int) -> int:
    pass


# 被装饰的函数一定是async def函数 
@client.register
async def sync_sum(a: int, b: int) -> int:
    pass


# 被装饰的函数一定是async def函数,由于该函数是生成器语法, 要以yield代替pass 
@client.register
async def async_gen(a: int):
    yield


async def main():
    await client.connect()
    # call调用,通过读取函数名再调用
    print(f"call result: {await client.call(sync_sum, 1, 2)}")
    # rap.client的基础调用
    print(f"raw call result: {await client.raw_call('sync_sum', 1, 2)}")
    
    # 通过@client.register注册的函数可以直接使用
    # await async_sum(1,3) 实际上等于 await client.raw_call('async_sum', 1, 2)
    # 建议使用@client.register方法,可以被IDE等工具自别参数类型是否有错
    print(f"decorator result: {await sync_sum(1, 3)}")
    async_gen_result: list = []
    
    # 异步生成器的例子, 默认会打开或者复用rap的当前session(关于session下面会说到)
    async for i in async_gen(10):
        async_gen_result.append(i)
    print(f"async gen result:{async_gen_result}")

    
asyncio.run(main())
```

# 2.功能介绍
## 2.1.session
[示例代码](https://github.com/so1n/rap/tree/master/example/session)

`rap`客户端支持会话功能, 在启用会话后,所有请求都只会通过当前会话的链接请求到对应的服务端,同时每次请求时,会在header的session_id设置当前会话id,方便服务端识别.
`rap`的会话支持显式设置和隐式设置,各有优缺点,不做强制限制.
```Python
import time

from rap.client import Client


client = Client()


def sync_sum(a: int, b: int) -> int:
    pass


@client.register
async def async_sum(a: int, b: int) -> int:
    pass


@client.register
async def async_gen(a: int):
    yield


async def no_param_run():
    # rap内部会通过contextvar模块隐式的调用到会话, 下面的调用方法与平常没有区别
    print(f"sync result: {await client.call(sync_sum, 1, 2)}")
    print(f"async result: {await async_sum(1, 3)}")

    # 异步生成器会自动复用当前的会话
    async for i in async_gen(10):
        print(f"async gen result:{i}")


async def param_run(session: "Session"):
    # 通过参数显式的把session传进去,被rap使用 
    print(f"sync result: {await client.call(sync_sum, 1, 2, session=session)}")
    print(f"sync result: {await client.raw_call('sync_sum', 1, 2, session=session)}")
    # 对于@client.register的调用方式有点不友好
    print(f"async result: {await async_sum(1, 3, session=session)}")

    # 异步生成器会自动复用当前的会话
    async for i in async_gen(10):
        print(f"async gen result:{i}")


async def execute(session: "Session"):
    # 使用类似于mysql cursor的方法进行调用,显式调用会话的最好方法
    # execute会自动识别调用类型
    print(f"sync result: {await session.execute(sync_sum, arg_list=[1, 2])}")
    print(f"sync result: {await session.execute('sync_sum', arg_list=[1, 2])}")
    print(f"async result: {await session.execute(async_sum(1, 3))}")

    # 异步生成器会自动复用当前的会话
    async for i in async_gen(10):
        print(f"async gen result:{i}")


async def run_once():
    await client.connect()
    # 初始化会话, 使用`async with`语法会优雅的关闭会话
    async with client.session as s:
        await no_param_run()
        await param_run(s)
        await execute(s)
    await client.wait_close()

```
## 2.2.channel
[示例代码](https://github.com/so1n/rap/tree/master/example/channel)

channel支持客户端与服务端以双工的方式进行交互,类似于Http的WebSocket.
客户端中只支持`@client.register`注册channel函数, channel函数的特点是函数的参数只有一个, 且类型为`Channel`:
channel会维持一个会话,在channel启用到关闭之前只会通过一个链接与服务端保持通信.
为了避免使用`while True`的情况,支持使用`async for`语法,同时支持使用`while await channel.loop()`语法代替`while True`
```Python
from rap.client import Channel, Client
from rap.client.model import Response

client = Client()


@client.register
async def async_channel(channel: Channel):
    await channel.write("hello")  # 发送数据
    cnt: int = 0
    while await channel.loop(cnt < 3):
        cnt += 1
        print(await channel.read_body())  # 读取数据
    return


@client.register
async def echo_body(channel: Channel):
    await channel.write("hi!")
    # 读取数据, 只有读取到数据才会返回, 如果收到关闭channel的信令, 则会退出循环
    async for body in channel.iter_body():
        print(f"body:{body}")
        await channel.write(body)


@client.register
async def echo_response(channel: Channel):
    await channel.write("hi!")
    # 读取响应数据(包括header等数据), 只有读取到数据才会返回, 如果收到关闭channel的信令, 则会退出循环
    async for response in channel.iter_response():
        response: Response = response  #  IDE 无法检查出该类型.... 
        print(f"response: {response}")
        await channel.write(response.body)
```
## 2.3.ssl支持
[示例代码](https://github.com/so1n/rap/tree/master/example/ssl)

得益于`Python asyncio`模块的封装, `rap`能非常方便的使用ssl
```bash
# 快速生成ssl.crt和ssl.key
openssl req -newkey rsa:2048 -nodes -keyout rap_ssl.key -x509 -days 365 -out rap_ssl.crt
```
客户端代码
```Python
from rap.client import Client

client = Client(ssl_crt_path="./rap_ssl.crt")
```
服务端代码
```Python
from rap.server import Server

rpc_server = Server(
    ssl_crt_path="./rap_ssl.crt",
    ssl_key_path="./rap_ssl.key",
)
```
## 2.4.中间件
`rap`目前支持2种中间件:
- 链接中间件: 创建链接时会使用,如限制链接总数等等...
  链接中间件可以参考[block.py](https://github.com/so1n/rap/blob/master/rap/server/middleware/conn/block.py),
  `dispatch`方法会传入一个链接对象,再根据规则判断是否放行(return await self.call_next(conn)) 或者拒绝(await conn.close)
- 消息中间件: 仅支持普通的函数调用(不支`持Channel`), 类似于`starlette`的中间件使用
  消息中间件可以参考[access.py](https://github.com/so1n/rap/blob/master/rap/server/middleware/msg/access.py)
  消息中间件会传入4个参数:request(当前请求对象), call_id(当前调用id), func(当前调用函数), param(当前参数)以及要求返回call_id和result(函数的执行结果或者是异常对象)
  
此外中间件还支持`start_event_handle`和`stop_event_handle`方法,分别在`Server`启动和关闭时调用.
`rap.server`引入中间件方法:
```Python
from rap.server import Server
from rap.server.middleware import AccessMsgMiddleware, ConnLimitMiddleware

rpc_server = Server()
rpc_server.load_middleware([ConnLimitMiddleware(), AccessMsgMiddleware()])
```
## 2.5.processor
`rap`的processor用于处理入站流量和出站流量,其中`process_request`是处理入站流量,`process_response`是处理出站流量.

`rap.client`和`rap.server`的processor的方法是基本一样的, `rap.server`支持`start_event_handle`和`stop_event_handle`方法,分别在`Server`启动和关闭时调用 
[服务端示例](https://github.com/so1n/rap/blob/master/rap/server/processor/crypto.py)
[客户端示例](https://github.com/so1n/rap/blob/master/rap/client/processor/crypto.py)
`rap.client` 引入processor方法
```Python
from rap.client import Client
from rap.client.processor import CryptoProcessor

client = Client()
client.load_processor([CryptoProcessor('key_id', 'xxxxxxxxxxxxxxxx')])
```
`rap.server`引入processor方法
```Python
from rap.server import Server
from rap.server.processor import CryptoProcessor

server = Server()
server.load_processor([CryptoProcessor({'key_id': 'xxxxxxxxxxxxxxxx'})])
```
## 2.6.event
在服务端中支持`start_event`和`stop_event`分别用于启动之前和关闭之后的事件处理.
如插件需要用到redis时,需要在rap.server启动之前启动,并在关闭之后关闭:
```Python
import aioredis
from rap.server import Server
from rap.manager.redis_manager import redis_manager


async def init_redis():
    conn_pool = await aioredis.create_pool("redis://localhost", minsize=1, maxsize=10, encoding="utf-8")
    redis_manager.init(conn_pool)


async def close_redis():
    await redis_manager.close()


# 方法一
server = Server(start_event_list=[init_redis()], stop_event_list=[close_redis()])
# 方法二
server = Server()
server.load_start_event([init_redis()])
server.load_stop_event([close_redis()])

```
# 3.插件
rap通过`middleware`和`processor`支持插件功能,`middleware`只支持服务端, `processor`支持客户端和服务端

## 3.1.加密传输
加密传输只加密请求和响应的body内容, 不对header等进行加密.在加密的同时会添加nonce参数,防止重放,添加timestamp参数防止超时访问.

客户端示例:
```Python
from rap.client import Client
from rap.client.processor import CryptoProcessor

client = Client()
# 第一个参数是秘钥的id, 服务端通过秘钥id判断当前的请求使用哪个秘钥
# 第二个参数是秘钥的key,目前仅支持长度为16位的秘钥
# timeout: 与当前timestamp对比超过timeout的值的请求会被抛弃
# interval: 清理nonce的间隔, 间隔越短执行越频繁,无用功越大, 间隔越长, 越容易占用内存, 推荐是timeout的2倍
client.load_processor([CryptoProcessor('demo_id', 'xxxxxxxxxxxxxxxx', timeout=60, interval=120)])
```
服务端示例:
```Python
from rap.server import Server
from rap.server.processor import CryptoProcessor


server = Server()
# 第一个参数为秘钥键值对,key为秘钥id, value为秘钥
# timeout: 与当前timestamp对比超过timeout的值的请求会被抛弃
# nonce_timeout: nonce的过期时间,最好大于timeout
server.load_processor([CryptoProcessor({"demo_id": "xxxxxxxxxxxxxxxx"}, timeout=60, nonce_timeout=120)])
```
## 3.2.限制最大链接数
仅限服务端使用,可以限制服务端的最大链接数,超过设定值则不会处理新的请求
```Python
from rap.server import Server
from rap.server.middleware import ConnLimitMiddleware, IpMaxConnMiddleware 


server = Server()
server.load_middleware(
    [
        # max_conn: 当前的最大链接数
        # block_timeout: 超过最大链接数后的禁止访问时间
        ConnLimitMiddleware(max_conn=100, block_time=60),
        # ip_max_conn: 每个ip的最大链接数
        # timeout: 统计周期, 如果超过该时间没有访问,相关IP的统计会被清零 
        IpMaxConnMiddleware(ip_max_conn=10, timeout=60),
    ]
)
```
## 3.3.限制ip访问
支持限制单个ip或者整个网段的ip, 同时支持白名单和黑名单模式,如果启用白名单,则默认禁用黑名单模式
```Python
from rap.server import Server
from rap.server.middleware import IpBlockMiddleware 


server = Server()
# allow_ip_list: 白名单列表,支持网段ip, 如果填了allow_ip_list, black_ip_list会失效
# black_ip_list: 黑名单列表,支持网段ip
server.load_middleware([IpBlockMiddleware(allow_ip_list=['192.168.0.0/31'], block_ip_list=['192.168.0.2'])])
```
# 4.高级功能
**TODO**, 本功能暂未实现

# 5.协议设计
**TODO**, 文档正在编辑中

# 6.底层传输介绍
**TODO**, 文档正在编辑中




