from typing import Optional

from rap.client.core import BaseClient
from rap.client.endpoint import BalanceEnum
from rap.client.endpoint.consul import ConsulEndpoint


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
        super().__init__(
            server_name,
            keep_alive_timeout=keep_alive_timeout,
            cache_interval=cache_interval,
            ws_min_interval=ws_min_interval,
            ws_max_interval=ws_max_interval,
            ws_statistics_interval=ws_statistics_interval,
            through_deadline=through_deadline,
        )
        self.endpoint = ConsulEndpoint(
            self,
            ssl_crt_path=ssl_crt_path,
            balance_enum=select_conn_method,
            ping_fail_cnt=ping_fail_cnt,
            min_ping_interval=min_ping_interval,
            max_ping_interval=max_ping_interval,
            max_pool_size=max_pool_size,
            min_poll_size=min_poll_size,
            consul_namespace=consul_namespace,
            consul_ttl=consul_ttl,
            consul_host=consul_host,
            consul_port=consul_port,
            consul_token=consul_token,
            consul_scheme=consul_scheme,
            consul_consistency=consul_consistency,
            consul_dc=consul_dc,
            consul_verify=consul_verify,
            consul_cert=consul_cert,
        )
