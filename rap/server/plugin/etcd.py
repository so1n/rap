from typing import TYPE_CHECKING, Optional

from rap.common.coordinator import EtcdClient

if TYPE_CHECKING:
    from rap.server import Server


def add_etcd_client(
    server: "Server",
    host: str = "localhost",
    port: int = 2379,
    ttl: int = 60,
    namespace: str = "rap",
    cert_path: Optional[str] = None,
    key_path: Optional[str] = None,
    ca_path: Optional[str] = None,
) -> "Server":
    etcd_client: EtcdClient = EtcdClient(
        host=host, port=port, ttl=ttl, namespace=namespace, cert_path=cert_path, key_path=key_path, ca_path=ca_path
    )

    async def register(app: "Server") -> None:
        await etcd_client.register(app.server_name, app.host, str(app.port))

    async def deregister(app: "Server") -> None:
        await etcd_client.deregister(app.server_name, app.host, str(app.port))

    server.load_start_event([register])
    server.load_stop_event([deregister])
    return server
