import asyncio

from rap.server import Server


def sum_num(a, b):
    return a + b


loop = asyncio.new_event_loop()
rpc_server = Server()
rpc_server.register(sum_num)
server = loop.run_until_complete(rpc_server.create_server())

try:
    loop.run_forever()
except KeyboardInterrupt:
    server.close()
    loop.run_until_complete(server.wait_closed())
