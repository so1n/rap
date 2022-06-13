import os
from typing import AsyncGenerator

import pytest

from rap.client import Client
from rap.client.endpoint import LocalEndpoint
from rap.server import Server

pwd_path: str = os.getcwd()
if not pwd_path.endswith("tests"):
    pwd_path += "/tests"
pytestmark = pytest.mark.asyncio
client: Client = Client(
    "test", endpoint=LocalEndpoint({"ip": "localhost", "port": 9000}, ssl_crt_path=f"{pwd_path}/rap_ssl.crt")
)


# in register, must use async def...
@client.register()
async def sync_sum(a: int, b: int) -> int:
    return 0


@pytest.fixture
async def ssl_client() -> AsyncGenerator[Client, None]:
    await client.start()
    yield client
    await client.stop()


@pytest.fixture
async def ssl_server() -> AsyncGenerator[Server, None]:
    def _sync_sum(a: int, b: int) -> int:
        return a + b

    rpc_server = Server(
        "test",
        # enable ssl
        ssl_crt_path=f"{pwd_path}/rap_ssl.crt",
        ssl_key_path=f"{pwd_path}/rap_ssl.key",
    )
    rpc_server.register(_sync_sum, "sync_sum")
    await rpc_server.create_server()
    yield rpc_server
    await rpc_server.shutdown()


class TestSSL:
    async def test_ssl(self, ssl_server: Server, ssl_client: Client) -> None:
        assert 3 == await sync_sum(1, 2)
