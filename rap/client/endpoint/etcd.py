import asyncio
import logging
from typing import Any, Dict, Optional

from rap.client.endpoint.base import BalanceEnum, BaseEndpoint
from rap.client.transport.transport import Transport
from rap.common.asyncio_helper import del_future, done_future
from rap.common.coordinator.etcd import ETCD_EVENT_VALUE_DICT_TYPE, EtcdClient

logger: logging.Logger = logging.getLogger()


class EtcdEndpoint(BaseEndpoint):
    """The endpoint will maintain the conn in memory according to the changes in the conn data in etcd"""

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
        # etcd client param
        etcd_host: str = "localhost",
        etcd_port: int = 2379,
        etcd_ttl: int = 60,
        etcd_namespace: str = "rap",
        etcd_cert_path: Optional[str] = None,
        etcd_key_path: Optional[str] = None,
        etcd_ca_path: Optional[str] = None,
    ):
        self.etcd_url: str = f"http://{etcd_host}:{etcd_port}"
        self.etcd_client: EtcdClient = EtcdClient(
            host=etcd_host,
            port=etcd_port,
            ttl=etcd_ttl,
            namespace=etcd_namespace,
            cert_path=etcd_cert_path,
            key_path=etcd_key_path,
            ca_path=etcd_ca_path,
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
        del_future(self._watch_future)
        await self.etcd_client.stop()
        await super().stop()

    async def start(self) -> None:
        """create conn by etcd info and init watch etcd info future"""
        if not self.is_close:
            raise ConnectionError(f"{self.__class__.__name__} is running")
        logger.info(f"connect to etcd:{self.etcd_url}, wait discovery....")
        async for item in self.etcd_client.discovery(self.server_name):
            await self.create(item["host"], item["port"], item["weight"])

        wait_start_future: asyncio.Future = asyncio.Future()
        if not self._conn_dict:
            logging.warning(
                f"Can not found conn info from etcd, wait {self.server_name} server start and register to etcd"
            )
        else:
            wait_start_future.set_result(True)

        _cache_dict: Dict[str, Any] = {}

        async def create(etcd_value_dict: ETCD_EVENT_VALUE_DICT_TYPE) -> None:
            _cache_dict[etcd_value_dict["key"]] = etcd_value_dict["value"]
            await self.create(
                etcd_value_dict["value"]["host"], etcd_value_dict["value"]["port"], etcd_value_dict["value"]["weight"]
            )
            if not wait_start_future.done():
                wait_start_future.set_result(True)

        async def destroy(etcd_value_dict: ETCD_EVENT_VALUE_DICT_TYPE) -> None:
            conn_dict: dict = _cache_dict.get(etcd_value_dict["key"], {})
            if not conn_dict:
                raise KeyError(f"Can not found key:{etcd_value_dict['key']}")
            await self.destroy(conn_dict["host"], conn_dict["port"])
            if not self._conn_dict:
                logging.warning("client not conn")

        self._watch_future = asyncio.ensure_future(self.etcd_client.watch(self.server_name, [create], [destroy]))
        await wait_start_future
