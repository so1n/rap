import asyncio
import logging
from typing import Optional

from rap.client.endpoint.base import BaseEndpoint, PickConnEnum
from rap.common.coordinator.consul import ConsulClient


class ConsulEndpoint(BaseEndpoint):
    """The endpoint will maintain the conn in memory according to the changes in the conn data in consul"""

    def __init__(
        self,
        server_name: str,
        timeout: Optional[int] = None,
        ssl_crt_path: Optional[str] = None,
        pack_param: Optional[dict] = None,
        unpack_param: Optional[dict] = None,
        pick_conn_method: PickConnEnum = PickConnEnum.random,
        ping_sleep_time: Optional[int] = None,
        ping_fail_cnt: Optional[int] = None,
        wait_server_recover: bool = True,
        # consul client param
        consul_namespace: str = "rap",
        consul_ttl: int = 10,
        consul_host: str = "127.0.0.1",
        consul_port: int = 8500,
        consul_token: Optional[str] = None,
        consul_scheme: str = "http",
        consul_consistency: str = "default",
        consul_dc: Optional[str] = None,
        consul_verify: bool = True,
        consul_cert: Optional[str] = None,
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
            cert=consul_cert,
        )
        self._watch_future: asyncio.Future = asyncio.Future()
        self._watch_future.set_result(True)
        super().__init__(
            timeout,
            ssl_crt_path,
            pick_conn_method,
            pack_param=pack_param,
            unpack_param=unpack_param,
            ping_fail_cnt=ping_fail_cnt,
            ping_sleep_time=ping_sleep_time,
            wait_server_recover=wait_server_recover,
        )
        self.server_name: str = server_name

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
                    del conn_dict[key]
            for key, value in conn_dict.items():
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
                    await self.create(value["host"], value["port"], value["weight"])
                    return
        self._watch_future = asyncio.ensure_future(self._watch())
        await super().start()
