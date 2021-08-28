import time
from typing import Any

import pytest
from pytest_mock import MockerFixture

from rap.client import Client
from rap.client.processor import CryptoProcessor
from rap.common.crypto import Crypto
from rap.common.exceptions import CryptoError
from rap.server import Server
from rap.server.plugin.processor import CryptoProcessor as ServerCryptoProcessor
from tests.conftest import async_sum, client  # type: ignore

pytestmark = pytest.mark.asyncio


class TestCrypto:
    def test_error_key_length(self) -> None:
        with pytest.raises(ValueError) as e:
            Crypto("demo")

        exec_msg: str = e.value.args[0]
        assert exec_msg.endswith("The length of the key must be 16, key content:demo")

    async def test_crypto_body_handle(self) -> None:
        crypto: CryptoProcessor = CryptoProcessor("test", "keyskeyskeyskeys")
        crypto.app = client
        demo_body_dict: dict = {"nonce": "aaa", "timestamp": int(time.time()) - 70}
        with pytest.raises(CryptoError) as e:
            crypto._body_handle(demo_body_dict)

        exec_msg: str = e.value.args[0]
        assert exec_msg == "timeout param error"

        demo_body_dict = {"nonce": "aaa", "timestamp": int(time.time())}
        crypto._body_handle(demo_body_dict)
        with pytest.raises(CryptoError) as e:
            crypto._body_handle(demo_body_dict)
        exec_msg = e.value.args[0]
        assert exec_msg == "nonce param error"


class TestServerCryptoProcess:
    async def test_process_response_error(self, rap_server: Server, rap_client: Client, mocker: Any) -> None:
        mocker.patch("rap.client.processor.CryptoProcessor._body_handle").side_effect = Exception()
        rap_client.load_processor([CryptoProcessor("test", "keyskeyskeyskeys")])
        rap_server.load_processor([ServerCryptoProcessor({"test": "keyskeyskeyskeys"})])
        with pytest.raises(CryptoError):
            await async_sum(1, 2)

    async def test_process_response_handle_error(self, rap_server: Server, rap_client: Client, mocker: Any) -> None:
        mocker.patch("rap.common.crypto.Crypto.decrypt_object").side_effect = Exception()
        rap_client.load_processor([CryptoProcessor("test", "keyskeyskeyskeys")])
        rap_server.load_processor([ServerCryptoProcessor({"test": "keyskeyskeyskeys"})])

        with pytest.raises(CryptoError) as e:
            await async_sum(1, 2)

        exec_msg = e.value.args[0]
        assert exec_msg == "Can't decrypt body."

    async def test_process_request_timestamp_param_error(self, rap_server: Server, rap_client: Client) -> None:
        rap_client.load_processor([CryptoProcessor("test", "keyskeyskeyskeys")])
        rap_server.load_processor([ServerCryptoProcessor({"test": "keyskeyskeyskeys"}, timeout=-1)])

        with pytest.raises(CryptoError) as e:
            await async_sum(1, 2)

        exec_msg = e.value.args[0]
        assert exec_msg == "Parse error. timeout param error"

    async def test_process_request_nonce_param_error(
        self, rap_server: Server, rap_client: Client, mocker: MockerFixture
    ) -> None:
        mocker.patch("rap.client.processor.crypto.get_snowflake_id").return_value = ""
        rap_client.load_processor([CryptoProcessor("test", "keyskeyskeyskeys")])
        rap_server.load_processor([ServerCryptoProcessor({"test": "keyskeyskeyskeys"})])

        with pytest.raises(CryptoError) as e:
            await async_sum(1, 2)

        exec_msg = e.value.args[0]
        assert exec_msg == "Parse error. nonce param error"

    async def test_process_request_nonce_repeat_param_error(
        self, rap_server: Server, rap_client: Client, mocker: MockerFixture
    ) -> None:
        mocker.patch("rap.client.processor.crypto.get_snowflake_id").return_value = "mocker"
        rap_client.load_processor([CryptoProcessor("test", "keyskeyskeyskeys")])
        rap_server.load_processor([ServerCryptoProcessor({"test": "keyskeyskeyskeys"})])

        with pytest.raises(CryptoError) as e:
            await async_sum(1, 2)
            await async_sum(1, 2)

        exec_msg = e.value.args[0]
        assert exec_msg == "Parse error. nonce param error"

    async def test_secret(self, rap_server: Server, rap_client: Client) -> None:
        rap_client.load_processor([CryptoProcessor("test", "keyskeyskeyskeys")])
        rap_server.load_processor([ServerCryptoProcessor({"test": "keyskeyskeyskeys"})])
        assert 3 == await async_sum(1, 2)

    async def test_secret_middleware_method(self, rap_server: Server, rap_client: Client) -> None:
        rap_client.load_processor([CryptoProcessor("test", "keyskeyskeyskeys")])
        middleware: ServerCryptoProcessor = ServerCryptoProcessor({"test": "keyskeyskeyskeys"})
        rap_server.load_processor([middleware])
        middleware.start_event_handle(rap_server)

        await rap_client.raw_invoke("modify_crypto_timeout", [10], group=middleware.__class__.__name__)
        assert middleware._timeout == 10
        await rap_client.raw_invoke("modify_crypto_nonce_timeout", [11], group=middleware.__class__.__name__)
        assert middleware._nonce_timeout == 11
        assert ["test"] == await rap_client.raw_invoke("get_crypto_key_id_list", group=middleware.__class__.__name__)
        await rap_client.raw_invoke(
            "load_aes_key_dict", [{"test1": "1234567890123456"}], group=middleware.__class__.__name__
        )
        await rap_client.raw_invoke("remove_aes", ["test1"], group=middleware.__class__.__name__)
