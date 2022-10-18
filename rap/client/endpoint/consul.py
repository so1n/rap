import asyncio
import logging
from typing import TYPE_CHECKING, List, Optional, Tuple, Type

from rap.client.endpoint.base import BalanceEnum, BaseEndpoint, BaseEndpointProvider
from rap.client.transport.pool import Pool, PoolProvider
from rap.common.asyncio_helper import done_future
from rap.common.coordinator.consul import ConsulClient

logger: logging.Logger = logging.getLogger(__name__)
if TYPE_CHECKING:
    from rap.client.core import BaseClient


class ConsulEndpoint(BaseEndpoint):
    """The endpoint will maintain the transport in memory according to the changes in the transport data in consul"""

    def __init__(
        self,
        consul_client: ConsulClient,
        balance_enum: BalanceEnum = BalanceEnum.random,
        pool_provider: Optional[PoolProvider] = None,
    ):
        self.consul_client: ConsulClient = consul_client
        self._watch_future: asyncio.Future = done_future()
        super().__init__(
            pool_provider=pool_provider,
            balance_enum=balance_enum,
        )

    async def stop(self) -> None:
        if not self._watch_future.done() and not self._watch_future.cancelled():
            self._watch_future.cancel()
        await self.consul_client.stop()
        await super().stop()

    async def _watch(self, server_name: str) -> None:
        async for conn_dict in self.consul_client.watch(server_name):
            if conn_dict:
                pop_key_list: List[Tuple[str, int]] = []
                for key, value in conn_dict:
                    pool: Optional[Pool] = self._transport_pool_dict.pop(key, None)
                    if pool:
                        await pool.destroy()
                        pop_key_list.append(key)
                for key in pop_key_list:
                    conn_dict.pop(key, None)

            for key, value in conn_dict.items():
                await self.create(value["host"], value["port"], value["weight"])

    async def start(self, app: "BaseClient") -> None:
        if not self.is_close:
            raise ConnectionError(f"{self.__class__.__name__} is running")

        logger.info(f"connect to consul:{self.consul_client.consul_url}, wait discovery....")
        async for item in self.consul_client.discovery(app.server_name):
            await self.create(
                app,
                item["host"],
                item["port"],
                weight=item["weight"],
                max_inflight=item.get("max_inflight"),
            )

        if not self._transport_key_list:
            logger.warning(
                f"Can not found transport info from consul,"
                f" wait `{app.server_name}` server start and register to consul"
            )
            async for conn_dict in self.consul_client.watch(app.server_name):
                for key, value in conn_dict.items():
                    await self.create(
                        app,
                        value["host"],
                        value["port"],
                        weight=value["weight"],
                        max_inflight=value.get("max_inflight"),
                    )
                    return
        self._start()
        self._watch_future = asyncio.ensure_future(self._watch(app.server_name))


class ConsulEndpointProvider(BaseEndpointProvider):
    @classmethod
    def build(
        cls,
        consul_client: ConsulClient,
        endpoint: Type[ConsulEndpoint] = ConsulEndpoint,
        balance_enum: Optional[BalanceEnum] = None,
    ) -> "ConsulEndpointProvider":
        return cls(endpoint, consul_client=consul_client, balance_enum=balance_enum)
