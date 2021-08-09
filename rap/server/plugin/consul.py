from typing import TYPE_CHECKING, Optional

from rap.common.coordinator.consul import ConsulClient
from rap.common.utils import EventEnum

if TYPE_CHECKING:
    from rap.server import Server


def add_consul_client(
    server: "Server",
    weight: int = 10,
    namespace: str = "rap",
    ttl: int = 10,
    host: str = "127.0.0.1",
    port: int = 8500,
    token: Optional[str] = None,
    scheme: str = "http",
    consistency: str = "default",
    dc: Optional[str] = None,
    verify: bool = True,
    cert: Optional[str] = None,
) -> "Server":
    """Tell consul-server its own service status"""
    consul_client: ConsulClient = ConsulClient(
        host=host,
        port=port,
        ttl=ttl,
        namespace=namespace,
        token=token,
        scheme=scheme,
        consistency=consistency,
        dc=dc,
        verify=verify,
        cert=cert,
    )

    async def register(app: "Server") -> None:
        await consul_client.register(app.server_name, app.host, str(app.port), weight)

    async def deregister(app: "Server") -> None:
        await consul_client.deregister(app.server_name, app.host, str(app.port))
        await consul_client.stop()

    server.register_server_event(EventEnum.after_start, register)
    server.register_server_event(EventEnum.before_end, deregister)
    return server
