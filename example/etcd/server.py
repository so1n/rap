import asyncio

from rap.common.coordinator import EtcdClient
from rap.server import Server


async def async_sum(a: int, b: int) -> int:
    await asyncio.sleep(1)  # mock io time
    return a + b


async def main() -> None:
    etcd_client: EtcdClient = EtcdClient()
    rpc_server = Server("example")
    rpc_server.register(async_sum)
    await etcd_client.register(rpc_server.server_name, rpc_server._local_ip, str(rpc_server._port))
    await rpc_server.run_forever()
    await etcd_client.deregister(rpc_server.server_name, rpc_server._local_ip, str(rpc_server._port))


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        format="[%(asctime)s %(levelname)s] %(message)s", datefmt="%y-%m-%d %H:%M:%S", level=logging.DEBUG
    )

    loop = asyncio.new_event_loop()
    loop.run_until_complete(main())
