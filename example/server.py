import asyncio

from rap.server import Server


def sync_sum(a: int, b: int):
    return a + b


async def async_sum(a: int, b: int):
    await asyncio.sleep(1)  # mock io time
    return a + b


async def async_gen(a):
    for i in range(a):
        yield i


if __name__ == '__main__':
    import logging
    logging.basicConfig(
        format='[%(asctime)s %(levelname)s] %(message)s',
        datefmt='%y-%m-%d %H:%M:%S',
        level=logging.DEBUG
    )

    loop = asyncio.new_event_loop()
    rpc_server = Server(secret_list=['keyskeyskeyskeys'])
    rpc_server.register(sync_sum)
    rpc_server.register(async_sum)
    rpc_server.register(async_gen)
    server = loop.run_until_complete(rpc_server.create_server())

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        server.close()
        loop.run_until_complete(server.wait_closed())
