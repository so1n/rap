import asyncio
import time
from typing import Any

import pytest
from aredis import StrictRedis  # type: ignore
from pytest_mock import MockerFixture

from rap.client import Client
from rap.client.processor import CryptoProcessor
from rap.common.crypto import Crypto
from rap.common.exceptions import CryptoError
from rap.server import Server
from rap.server.processor import CryptoProcessor as ServerCryptoProcessor

from .conftest import async_sum  # type: ignore

pytestmark = pytest.mark.asyncio


class TestCrypto:
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


class TestServerCryptoProcess:

    async def test_process_response_error(self, rap_server: Server, rap_client: Client, mocker: Any) -> None:
        mocker.patch("rap.client.processor.CryptoProcessor._body_handle").side_effect = Exception()
        redis: StrictRedis = StrictRedis.from_url("redis://localhost")
        rap_client.load_processor([CryptoProcessor("test", "keyskeyskeyskeys")])
        rap_server.load_processor([ServerCryptoProcessor({"test": "keyskeyskeyskeys"}, redis)])
        with pytest.raises(CryptoError):
            await async_sum(1, 2)

    async def test_process_response_handle_error(self, rap_server: Server, rap_client: Client, mocker: Any) -> None:
        mocker.patch("rap.common.crypto.Crypto.decrypt_object").side_effect = Exception()
        redis: StrictRedis = StrictRedis.from_url("redis://localhost")
        rap_client.load_processor([CryptoProcessor("test", "keyskeyskeyskeys")])
        rap_server.load_processor([ServerCryptoProcessor({"test": "keyskeyskeyskeys"}, redis)])

        with pytest.raises(CryptoError) as e:
            await async_sum(1, 2)

        exec_msg = e.value.args[0]
        assert exec_msg == "decrypt body error"

    async def test_process_request_timestamp_param_error(
            self, rap_server: Server, rap_client: Client, mocker: Any
    ) -> None:
        redis: StrictRedis = StrictRedis.from_url("redis://localhost")
        rap_client.load_processor([CryptoProcessor("test", "keyskeyskeyskeys")])
        rap_server.load_processor([ServerCryptoProcessor({"test": "keyskeyskeyskeys"}, redis, timeout=-1)])

        with pytest.raises(CryptoError) as e:
            await async_sum(1, 2)

        exec_msg = e.value.args[0]
        assert exec_msg == "Parse error. timeout param error"

    async def test_process_request_nonce_param_error(
            self, rap_server: Server, rap_client: Client, mocker: MockerFixture
    ) -> None:
        mocker.patch("rap.client.processor.crypto.gen_random_time_id").return_value = ""
        redis: StrictRedis = StrictRedis.from_url("redis://localhost")
        rap_client.load_processor([CryptoProcessor("test", "keyskeyskeyskeys")])
        rap_server.load_processor([ServerCryptoProcessor({"test": "keyskeyskeyskeys"}, redis)])

        with pytest.raises(CryptoError) as e:
            await async_sum(1, 2)

        exec_msg = e.value.args[0]
        assert exec_msg == "Parse error. nonce param error"

    async def test_process_request_nonce_repeat_param_error(
            self, rap_server: Server, rap_client: Client, mocker: MockerFixture
    ) -> None:
        mocker.patch("rap.client.processor.crypto.gen_random_time_id").return_value = "mocker"
        redis: StrictRedis = StrictRedis.from_url("redis://localhost")
        rap_client.load_processor([CryptoProcessor("test", "keyskeyskeyskeys")])
        rap_server.load_processor([ServerCryptoProcessor({"test": "keyskeyskeyskeys"}, redis)])

        with pytest.raises(CryptoError) as e:
            await async_sum(1, 2)
            await async_sum(1, 2)

        exec_msg = e.value.args[0]
        assert exec_msg == "Parse error. nonce param error"

    async def test_process_request_redis_error(
            self, rap_server: Server, rap_client: Client, mocker: MockerFixture
    ) -> None:
        future: asyncio.Future = asyncio.Future()
        mocker.patch("rap.server.processor.crypto.StrictRedis.exists").return_value = future
        future.set_exception(Exception("customer exc"))

        redis: StrictRedis = StrictRedis.from_url("redis://localhost")
        rap_client.load_processor([CryptoProcessor("test", "keyskeyskeyskeys")])
        rap_server.load_processor([ServerCryptoProcessor({"test": "keyskeyskeyskeys"}, redis)])

        with pytest.raises(CryptoError) as e:
            await async_sum(1, 2)

        exec_msg = e.value.args[0]
        assert exec_msg == "customer exc"

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
