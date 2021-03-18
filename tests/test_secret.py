import time

import pytest
from aredis import StrictRedis

from rap.client import Client
from rap.client.processor import CryptoProcessor
from rap.common.crypto import Crypto
from rap.common.exceptions import CryptoError
from rap.server import Server
from rap.server.processor import CryptoProcessor as ServerCryptoProcessor

from .conftest import async_sum

pytestmark = pytest.mark.asyncio


class TestSecret:
    def test_error_key_length(self) -> None:
        with pytest.raises(ValueError) as e:
            Crypto("demo")

        exec_msg: str = e.value.args[0]
        assert exec_msg.endswith("The length of the key must be 16, key content:demo")

    async def test_crypto_body_handle(self) -> None:
        crypto: CryptoProcessor = CryptoProcessor("test", "keyskeyskeyskeys")
        demo_body_dict: dict = {"nonce": "aaa", "timestamp": int(time.time()) - 70}
        with pytest.raises(CryptoError) as e:
            crypto._body_handle(demo_body_dict)

        exec_msg: str = e.value.args[0]
        assert exec_msg == "timeout error"

        demo_body_dict = {"nonce": "aaa", "timestamp": int(time.time())}
        crypto._body_handle(demo_body_dict)
        with pytest.raises(CryptoError) as e:
            crypto._body_handle(demo_body_dict)
        exec_msg = e.value.args[0]
        assert exec_msg == "nonce error"

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

        await rap_client.raw_call("modify_crypto_timeout", [10], group=middleware.__class__.__name__)
        assert middleware._timeout == 10
        await rap_client.raw_call("modify_crypto_nonce_timeout", [11], group=middleware.__class__.__name__)
        assert middleware._nonce_timeout == 11
        assert ["test"] == await rap_client.raw_call("get_crypto_key_id_list", group=middleware.__class__.__name__)
        await rap_client.raw_call(
            "load_aes_key_dict", [{"test1": "1234567890123456"}], group=middleware.__class__.__name__
        )
        await rap_client.raw_call("remove_aes", ["test1"], group=middleware.__class__.__name__)
