from typing import Optional

from rap.client.core import BaseClient
from rap.client.endpoint import SelectConnEnum
from rap.client.endpoint.etcd import EtcdEndpoint


class Client(BaseClient):
    def __init__(
        self,
        server_name: str,
        timeout: int = 9,
        keep_alive_timeout: int = 1200,
        ssl_crt_path: Optional[str] = None,
        select_conn_method: SelectConnEnum = SelectConnEnum.random,
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
            EtcdEndpoint(
                server_name,
                timeout=keep_alive_timeout,
                ssl_crt_path=ssl_crt_path,
                select_conn_method=select_conn_method,
                etcd_host=etcd_host,
                etcd_port=etcd_port,
                etcd_ttl=etcd_ttl,
                etcd_namespace=etcd_namespace,
                etcd_cert_path=etcd_cert_path,
                etcd_key_path=etcd_key_path,
                etcd_ca_path=etcd_ca_path,
            ),
            timeout,
        )
