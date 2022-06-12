from typing import Optional

from rap.client.core import BaseClient
from rap.client.endpoint import BalanceEnum
from rap.client.endpoint.etcd import EtcdEndpoint


class Client(BaseClient):
    def __init__(
        self,
        server_name: str,
        keep_alive_timeout: int = 1200,
        ssl_crt_path: Optional[str] = None,
        cache_interval: Optional[float] = None,
        ws_min_interval: Optional[int] = None,
        ws_max_interval: Optional[int] = None,
        ws_statistics_interval: Optional[int] = None,
        select_conn_method: BalanceEnum = BalanceEnum.random,
        min_ping_interval: Optional[int] = None,
        max_ping_interval: Optional[int] = None,
        ping_fail_cnt: Optional[int] = None,
        through_deadline: bool = False,
        max_pool_size: Optional[int] = None,
        min_poll_size: Optional[int] = None,
        # etcd client param
        etcd_host: str = "localhost",
        etcd_port: int = 2379,
        etcd_ttl: int = 60,
        etcd_namespace: str = "rap",
        etcd_cert_path: Optional[str] = None,
        etcd_key_path: Optional[str] = None,
        etcd_ca_path: Optional[str] = None,
    ):
        super().__init__(
            server_name,
            cache_interval=cache_interval,
            ws_min_interval=ws_min_interval,
            ws_max_interval=ws_max_interval,
            ws_statistics_interval=ws_statistics_interval,
            through_deadline=through_deadline,
        )
        self._endpoint = EtcdEndpoint(
            self,
            ssl_crt_path=ssl_crt_path,
            read_timeout=keep_alive_timeout,
            balance_enum=select_conn_method,
            ping_fail_cnt=ping_fail_cnt,
            min_ping_interval=min_ping_interval,
            max_ping_interval=max_ping_interval,
            max_pool_size=max_pool_size,
            min_poll_size=min_poll_size,
            etcd_host=etcd_host,
            etcd_port=etcd_port,
            etcd_ttl=etcd_ttl,
            etcd_namespace=etcd_namespace,
            etcd_cert_path=etcd_cert_path,
            etcd_key_path=etcd_key_path,
            etcd_ca_path=etcd_ca_path,
        )
