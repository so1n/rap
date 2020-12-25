import asyncio

from rap.server import Server
from rap.server.requests import Channel


async def async_channel(channel: Channel):
    while not channel.is_close:
        logging.info(channel.is_close)
        body: any = await channel.read_body()
        if body == 'hello':
            cnt: int = 0
            while True:
                cnt += 1
                await channel.write(f'hello {cnt}')
        else:
            await channel.write("I don't know")


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        format="[%(asctime)s %(levelname)s] %(message)s", datefmt="%y-%m-%d %H:%M:%S", level=logging.DEBUG
    )

    loop = asyncio.new_event_loop()
    rpc_server = Server()
    rpc_server.register(async_channel)

    loop.run_until_complete(rpc_server.create_server())
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        loop.run_until_complete(rpc_server.wait_closed())
