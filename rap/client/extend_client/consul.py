from typing import Optional

from rap.client.core import BaseClient
from rap.client.endpoint import SelectConnEnum
from rap.client.endpoint.consul import ConsulEndpoint


class Client(BaseClient):
    def __init__(
        self,
        server_name: str,
        timeout: int = 9,
        keep_alive_timeout: int = 1200,
        ssl_crt_path: Optional[str] = None,
        cache_interval: Optional[float] = None,
        ws_min_interval: Optional[int] = None,
        ws_max_interval: Optional[int] = None,
        ws_statistics_interval: Optional[int] = None,
        select_conn_method: SelectConnEnum = SelectConnEnum.random,
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
            ConsulEndpoint(
                server_name,
                timeout=keep_alive_timeout,
                ssl_crt_path=ssl_crt_path,
                select_conn_method=select_conn_method,
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
            ),
            timeout,
            cache_interval=cache_interval,
            ws_min_interval=ws_min_interval,
            ws_max_interval=ws_max_interval,
            ws_statistics_interval=ws_statistics_interval,
        )
