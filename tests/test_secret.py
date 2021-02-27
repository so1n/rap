import pytest

from aredis import StrictRedis
from rap.client import Client
from rap.client.processor import CryptoProcessor
from rap.server import Server
from rap.server.processor import CryptoProcessor as ServerCryptoProcessor

from .conftest import async_sum

pytestmark = pytest.mark.asyncio


class TestSecret:
    async def test_secret(self, rap_server: Server, rap_client: Client) -> None:
        redis: StrictRedis = StrictRedis.from_url("redis://localhost")
        rap_client.load_processor([CryptoProcessor("test", "keyskeyskeyskeys")])
        rap_server.load_processor([ServerCryptoProcessor({"test": "keyskeyskeyskeys"}, redis)])
        assert 3 == await async_sum(1, 2)
