import asyncio
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
from tests.conftest import process_client  # type: ignore

pytestmark = pytest.mark.asyncio


class TestCrypto:
    def test_error_key_length(self) -> None:
        with pytest.raises(ValueError) as e:
            Crypto("demo")

        exec_msg: str = e.value.args[0]
        assert exec_msg.endswith("The length of the key must be 16, key content:demo")

    async def test_crypto_body_handle(self) -> None:
        crypto: CryptoProcessor = CryptoProcessor("test", "keyskeyskeyskeys")
        crypto.app = Client("test", [{"ip": "localhost", "port": "9000"}])
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
    async def test_process_response_error(self, rap_server: Server, mocker: Any) -> None:
        mocker.patch("rap.client.processor.CryptoProcessor._body_handle").side_effect = Exception()
        rap_server.load_processor([ServerCryptoProcessor({"test": "keyskeyskeyskeys"})])
        async with process_client([CryptoProcessor("test", "keyskeyskeyskeys")]):
            from tests.conftest import async_sum

            with pytest.raises(CryptoError):
                await async_sum(1, 2)

    # async def test_process_response_handle_error(self, rap_server: Server, mocker: Any) -> None:
    #     rap_server.load_processor([ServerCryptoProcessor({"test": "keyskeyskeyskeys"})])
    #
    #     async with process_client([CryptoProcessor("test", "keyskeyskeyskeys")]):
    #         mocker.patch("rap.common.crypto.Crypto.decrypt_object").side_effect = Exception()
    #         with pytest.raises(CryptoError) as e:
    #             await async_sum(1, 2)
    #
    #         exec_msg = e.value.args[0]
    #         assert exec_msg == "Can't decrypt body."

    async def test_process_request_timestamp_param_error(self, rap_server: Server) -> None:
        rap_server.load_processor([ServerCryptoProcessor({"test": "keyskeyskeyskeys"}, timeout=-1)])
        async with process_client([CryptoProcessor("test", "keyskeyskeyskeys")]):
            with pytest.raises(CryptoError) as e:
                from tests.conftest import async_sum

                await async_sum(1, 2)

            exec_msg = e.value.args[0]
            assert exec_msg == "Parse error. timeout param error"

    async def test_process_request_nonce_param_error(self, rap_server: Server, mocker: MockerFixture) -> None:
        future: asyncio.Future = asyncio.Future()
        mocker.patch("rap.client.processor.crypto.async_get_snowflake_id").return_value = future
        future.set_result("")
        rap_server.load_processor([ServerCryptoProcessor({"test": "keyskeyskeyskeys"})])

        async with process_client([CryptoProcessor("test", "keyskeyskeyskeys")]):
            from tests.conftest import async_sum

            with pytest.raises(CryptoError) as e:
                await async_sum(1, 2)

            exec_msg = e.value.args[0]
            assert exec_msg == "Parse error. nonce param error"

    async def test_process_request_nonce_repeat_param_error(self, rap_server: Server, mocker: MockerFixture) -> None:
        future: asyncio.Future = asyncio.Future()
        mocker.patch("rap.client.processor.crypto.async_get_snowflake_id").return_value = future
        future.set_result("mocker")

        rap_server.load_processor([ServerCryptoProcessor({"test": "keyskeyskeyskeys"})])
        async with process_client([CryptoProcessor("test", "keyskeyskeyskeys")]):
            from tests.conftest import async_sum

            assert await async_sum(1, 2) == 3
            with pytest.raises(CryptoError) as e:
                await async_sum(1, 2)

            exec_msg = e.value.args[0]
            assert exec_msg == "Parse error. nonce param error"

    async def test_secret(self, rap_server: Server) -> None:
        rap_server.load_processor([ServerCryptoProcessor({"test": "keyskeyskeyskeys"})])
        async with process_client([CryptoProcessor("test", "keyskeyskeyskeys")]):
            from tests.conftest import async_sum

            assert 3 == await async_sum(1, 2)

    async def test_secret_middleware_method(self, rap_server: Server) -> None:
        middleware: ServerCryptoProcessor = ServerCryptoProcessor({"test": "keyskeyskeyskeys"})
        rap_server.load_processor([middleware])
        middleware.start_event_handle(rap_server)
        async with process_client([CryptoProcessor("test", "keyskeyskeyskeys")]) as rap_client:

            await rap_client.raw_invoke("modify_crypto_timeout", [10], group=middleware.__class__.__name__)
            assert middleware._timeout == 10
            await rap_client.raw_invoke("modify_crypto_nonce_timeout", [11], group=middleware.__class__.__name__)
            assert middleware._nonce_timeout == 11
            assert ["test"] == await rap_client.raw_invoke(
                "get_crypto_key_id_list", group=middleware.__class__.__name__
            )
            await rap_client.raw_invoke(
                "load_aes_key_dict", [{"test1": "1234567890123456"}], group=middleware.__class__.__name__
            )
            await rap_client.raw_invoke("remove_aes", ["test1"], group=middleware.__class__.__name__)
