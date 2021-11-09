import asyncio
import logging
from typing import Optional

from rap.client.endpoint.base import BalanceEnum, BaseEndpoint
from rap.client.transport.transport import Transport
from rap.common.asyncio_helper import done_future
from rap.common.coordinator.consul import ConsulClient

logger: logging.Logger = logging.getLogger(__name__)


class ConsulEndpoint(BaseEndpoint):
    """The endpoint will maintain the conn in memory according to the changes in the conn data in consul"""

    def __init__(
        self,
        server_name: str,
        transport: Transport,
        ssl_crt_path: Optional[str] = None,
        pack_param: Optional[dict] = None,
        unpack_param: Optional[dict] = None,
        balance_enum: BalanceEnum = BalanceEnum.random,
        min_ping_interval: Optional[int] = None,
        max_ping_interval: Optional[int] = None,
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
        self.consul_url: str = f"{consul_scheme}://{consul_host}:{consul_port}"
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
        self._watch_future: asyncio.Future = done_future()
        super().__init__(
            transport,
            ssl_crt_path=ssl_crt_path,
            balance_enum=balance_enum,
            pack_param=pack_param,
            unpack_param=unpack_param,
            ping_fail_cnt=ping_fail_cnt,
            min_ping_interval=min_ping_interval,
            max_ping_interval=max_ping_interval,
            wait_server_recover=wait_server_recover,
        )
        self.server_name: str = server_name

    async def stop(self) -> None:
        if not self._watch_future.done() and not self._watch_future.cancelled():
            self._watch_future.cancel()
        await self.consul_client.stop()
        await super().stop()

    async def _watch(self) -> None:
        async for conn_dict in self.consul_client.watch(self.server_name):
            if conn_dict:
                for key, conn in self._conn_dict.items():
                    if key not in conn_dict:
                        await self.destroy(conn.host, conn.port)
                    else:
                        conn_dict.pop(key, None)
            for key, value in conn_dict.items():
                await self.create(value["host"], value["port"], value["weight"])

    async def start(self) -> None:
        if not self.is_close:
            raise ConnectionError(f"{self.__class__.__name__} is running")

        logger.info(f"connect to consul:{self.consul_url}, wait discovery....")
        async for item in self.consul_client.discovery(self.server_name):
            await self.create(item["host"], item["port"], item["weight"])

        if not self._conn_dict:
            logger.warning(
                f"Can not found conn info from cousul, wait `{self.server_name}` server start and register to consul"
            )
            async for conn_dict in self.consul_client.watch(self.server_name):
                for key, value in conn_dict.items():
                    await self.create(value["host"], value["port"], value["weight"])
                    return
        self._watch_future = asyncio.ensure_future(self._watch())
