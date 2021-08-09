from typing import TYPE_CHECKING, Optional

from rap.common.coordinator.etcd import EtcdClient
from rap.common.utils import EventEnum

if TYPE_CHECKING:
    from rap.server import Server


def add_etcd_client(
    server: "Server",
    weight: int = 10,
    host: str = "localhost",
    port: int = 2379,
    ttl: int = 60,
    namespace: str = "rap",
    cert_path: Optional[str] = None,
    key_path: Optional[str] = None,
    ca_path: Optional[str] = None,
) -> "Server":
    """Tell etcd-server its own service status"""
    etcd_client: EtcdClient = EtcdClient(
        host=host, port=port, ttl=ttl, namespace=namespace, cert_path=cert_path, key_path=key_path, ca_path=ca_path
    )

    async def register(app: "Server") -> None:
        await etcd_client.register(app.server_name, app.host, str(app.port), weight)

    async def deregister(app: "Server") -> None:
        await etcd_client.deregister(app.server_name, app.host, str(app.port))
        await etcd_client.stop()

    server.register_server_event(EventEnum.after_start, register)
    server.register_server_event(EventEnum.before_end, deregister)
    return server
