import asyncio

from rap.server import Server
from rap.server.requests import Channel


async def async_channel(channel: Channel):
    while await channel.loop():
        body: any = await channel.read_body()
        if body == 'hello':
            cnt: int = 0
            await channel.write(f'hello {cnt}')
            while await channel.loop(cnt < 10):
                cnt += 1
                await channel.write(f'hello {cnt}')
        else:
            await channel.write("I don't know")


async def echo(channel: Channel):
    cnt: int = 0
    async for body in channel:
        await asyncio.sleep(1)
        cnt += 1
        if cnt > 10:
            break
        await channel.write(body)


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        format="[%(asctime)s %(levelname)s] %(message)s", datefmt="%y-%m-%d %H:%M:%S", level=logging.INFO
    )

    loop = asyncio.new_event_loop()
    rpc_server = Server()
    rpc_server.register(async_channel)
    rpc_server.register(echo)

    loop.run_until_complete(rpc_server.create_server())
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        loop.run_until_complete(rpc_server.wait_closed())
