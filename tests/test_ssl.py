from typing import AsyncGenerator
import pytest
from rap.client import Client
from rap.server import Server

pytestmark = pytest.mark.asyncio
client = Client(ssl_crt_path="./tests/rap_ssl.crt")  # enable secret


# in register, must use async def...
@client.register()
async def sync_sum(a: int, b: int) -> int:
    pass


@pytest.fixture
async def ssl_client() -> AsyncGenerator[Client, None]:
    await client.connect()
    yield client
    await client.await_close()


@pytest.fixture
async def ssl_server() -> AsyncGenerator[Server, None]:
    def _sync_sum(a: int, b: int) -> int:
        return a + b

    rpc_server = Server(
            # enable ssl
            ssl_crt_path="./tests/rap_ssl.crt",
            ssl_key_path="./tests/rap_ssl.key",
        )
    rpc_server.register(_sync_sum, "sync_sum")
    await rpc_server.create_server()
    yield rpc_server
    await rpc_server.await_closed()


class TestSSL:
    async def test_ssl(self, ssl_server: Server, ssl_client: Client) -> None:
        assert 3 == await sync_sum(1, 2)
