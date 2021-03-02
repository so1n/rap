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

    async def test_secret_middleware_method(self, rap_server: Server, rap_client: Client) -> None:
        redis: StrictRedis = StrictRedis.from_url("redis://localhost")
        rap_client.load_processor([CryptoProcessor("test", "keyskeyskeyskeys")])
        middleware: ServerCryptoProcessor = ServerCryptoProcessor({"test": "keyskeyskeyskeys"}, redis)
        rap_server.load_processor([middleware])
        middleware.start_event_handle()

        await rap_client.raw_call("modify_crypto_timeout", 10, group=middleware.__class__.__name__)
        assert middleware._timeout == 10
        await rap_client.raw_call("modify_crypto_nonce_timeout", 11, group=middleware.__class__.__name__)
        assert middleware._nonce_timeout == 11
        assert ["test"] == await rap_client.raw_call("get_crypto_key_id_list", group=middleware.__class__.__name__)
        await rap_client.raw_call(
            "load_aes_key_dict", {"test1": "1234567890123456"}, group=middleware.__class__.__name__
        )
        await rap_client.raw_call("remove_aes", "test1", group=middleware.__class__.__name__)



