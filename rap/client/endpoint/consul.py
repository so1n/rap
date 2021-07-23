import asyncio
import logging
from typing import Optional

from rap.client.endpoint.base import BaseEndpoint, SelectConnEnum
from rap.common.coordinator.consul import ConsulClient


class ConsulEndpoint(BaseEndpoint):
    def __init__(
        self,
        server_name: str,
        timeout: int = 9,
        ssl_crt_path: Optional[str] = None,
        select_conn_method: SelectConnEnum = SelectConnEnum.random,
        # consul client param
        consul_namespace: str = "rap",
        consul_ttl: int = 10,
        consul_host: str = '127.0.0.1',
        consul_port: int = 8500,
        consul_token: Optional[str] = None,
        consul_scheme: str = 'http',
        consul_consistency: str = 'default',
        consul_dc: Optional[str] = None,
        consul_verify: bool = True,
        consul_cert: Optional[str] = None
    ):
        self.consul_client: ConsulClient = ConsulClient(
            namespace=consul_namespace,
            ttl=consul_ttl,
            host=consul_host,
            port=consul_port,
            token=consul_token,
            scheme=consul_scheme,
            consistency=consul_consistency,
            dc=consul_dc,
            verify=consul_verify,
            cert=consul_cert
        )
        self._watch_future: asyncio.Future = asyncio.Future()
        self._watch_future.set_result(True)
        super().__init__(server_name, timeout, ssl_crt_path, select_conn_method)

    async def stop(self) -> None:
        await super().stop()
        if not self._watch_future.done() and not self._watch_future.cancelled():
            self._watch_future.cancel()
        await self.consul_client.stop()

    async def _watch(self) -> None:
        async for conn_dict in self.consul_client.watch(self.server_name):
            if conn_dict:
                for key, value in conn_dict.items():
                    if key not in conn_dict:
                        await self.destroy(value["host"], value["port"])
                    print('del', key)
                    del conn_dict[key]
            for key, value in conn_dict.items():
                print(key)
                await self.create(value["host"], value["port"], value["weight"])

    async def start(self) -> None:
        if not self.is_close:
            raise ConnectionError(f"{self.__class__.__name__} is running")
        async for item in self.consul_client.discovery(self.server_name):
            await self.create(item["host"], item["port"])

        if not self._conn_dict:
            logging.warning(f"Can not found conn info from etcd, wait {self.server_name} server start")
            async for conn_dict in self.consul_client.watch(self.server_name):
                for key, value in conn_dict.items():
                    # TODO fix server start event
                    await asyncio.sleep(2)
                    await self.create(value["host"], value["port"], value["weight"])
                    return
        self._watch_future = asyncio.ensure_future(self._watch())
