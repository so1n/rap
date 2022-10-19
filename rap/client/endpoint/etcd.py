import asyncio
import logging
from typing import Any, Dict, Optional, Tuple, Type

from rap.client.endpoint.base import BalanceEnum, BaseEndpoint, BaseEndpointProvider
from rap.client.transport.pool import Pool, PoolProvider
from rap.common.asyncio_helper import del_future, done_future
from rap.common.coordinator.etcd import ETCD_EVENT_VALUE_DICT_TYPE, EtcdClient

logger: logging.Logger = logging.getLogger(__name__)


class EtcdEndpoint(BaseEndpoint):
    """The endpoint will maintain the transport in memory according to the changes in the transport data in etcd"""

    def __init__(
        self,
        etcd_client: EtcdClient,
        server_name: str,
        pool_provider: Optional[PoolProvider] = None,
        balance_enum: BalanceEnum = BalanceEnum.random,
    ):
        self._server_name: str = server_name
        self.etcd_client: EtcdClient = etcd_client
        self._watch_future: asyncio.Future = done_future()
        super().__init__(
            balance_enum=balance_enum,
            pool_provider=pool_provider,
        )

    async def stop(self) -> None:
        del_future(self._watch_future)
        await self.etcd_client.stop()
        await super().stop()

    async def start(self) -> None:
        """create transport by etcd info and init watch etcd info future"""
        if not self.is_close:
            raise ConnectionError(f"{self.__class__.__name__} is running")
        logger.info(f"connect to etcd:{self.etcd_client.etcd_url}, wait discovery....")
        async for item in self.etcd_client.discovery(self._server_name):
            await self.create(
                item["host"],
                item["port"],
                weight=item["weight"],
                max_inflight=item.get("max_inflight"),
            )

        wait_start_future: asyncio.Future = asyncio.Future()
        if not self._transport_key_list:
            logger.warning(
                f"Can not found transport info from etcd,"
                f" wait {self._server_name} server start and register to etcd"
            )
        else:
            wait_start_future.set_result(True)

        _cache_dict: Dict[str, Any] = {}

        async def create(etcd_value_dict: ETCD_EVENT_VALUE_DICT_TYPE) -> None:
            _cache_dict[etcd_value_dict["key"]] = etcd_value_dict["value"]
            await self.create(
                etcd_value_dict["value"]["host"],
                etcd_value_dict["value"]["port"],
                weight=etcd_value_dict["value"]["weight"],
                max_inflight=etcd_value_dict["value"]["max_inflight"],
            )
            if not wait_start_future.done():
                wait_start_future.set_result(True)

        async def destroy(etcd_value_dict: ETCD_EVENT_VALUE_DICT_TYPE) -> None:
            conn_dict: dict = _cache_dict.pop(etcd_value_dict["key"], {})
            if not conn_dict:
                raise KeyError(f"Can not found key:{etcd_value_dict['key']}")
            key: Tuple[str, int] = (conn_dict["host"], conn_dict["post"])
            pool: Optional[Pool] = self._transport_pool_dict.pop(key, None)
            if pool:
                await pool.destroy()
            if not self._transport_key_list:
                logger.warning("client not transport")

        self._start()
        self._watch_future = asyncio.ensure_future(self.etcd_client.watch(self._server_name, [create], [destroy]))
        await wait_start_future


class EtcdEndpointProvider(BaseEndpointProvider):
    @classmethod
    def build(
        cls,
        etcd_client: EtcdClient,
        server_name: str,
        endpoint: Type[EtcdEndpoint] = EtcdEndpoint,
        balance_enum: Optional[BalanceEnum] = None,
    ) -> "EtcdEndpointProvider":
        return cls(endpoint, server_name=server_name, etcd_client=etcd_client, balance_enum=balance_enum)
