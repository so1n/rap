import logging
from typing import TYPE_CHECKING, Optional

from rap.common.coordinator.etcd import EtcdClient
from rap.common.utils import EventEnum

if TYPE_CHECKING:
    from rap.server import Server

logger: logging.Logger = logging.getLogger(__name__)


def add_etcd_client(
    config_name: str,
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
    logger.info(f"connect cousul server:<http://{host}:{port}>")

    async def register(app: "Server") -> None:
        await etcd_client.register(config_name, app.host, str(app.port), weight)
        logger.info(f"register to etcd success host:{app.host} port:{app.port} weight:{weight}")

    async def deregister(app: "Server") -> None:
        await etcd_client.deregister(config_name, app.host, str(app.port))
        logger.info("deregister from etcd success")
        await etcd_client.stop()

    server.register_server_event(EventEnum.after_start, register)
    server.register_server_event(EventEnum.before_end, deregister)
    return server
