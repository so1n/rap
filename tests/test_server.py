import asyncio

import pytest
from aredis import StrictRedis  # type: ignore
from pytest_mock import MockerFixture

from rap.client import Client
from rap.common.exceptions import RpcRunTimeError, ServerError
from rap.server import Server
from rap.server.plugin.middleware.msg.access import AccessMsgMiddleware
from rap.server.plugin.processor import CryptoProcessor as ServerCryptoProcessor

pytestmark = pytest.mark.asyncio

redis: StrictRedis = StrictRedis.from_url("redis://localhost")


class TestServerEvent:
    def test_load_event_by_init(self) -> None:
        async def demo_start_event() -> None:
            pass

        async def demo_stop_event() -> None:
            pass

        rap_server: Server = Server(start_event_list=[demo_start_event], stop_event_list=[demo_stop_event])
        assert rap_server._start_event_list[0] == demo_start_event
        assert rap_server._stop_event_list[0] == demo_stop_event

    def test_load_error_event(self) -> None:

        with pytest.raises(ImportError):
            Server(start_event_list=["111"])  # type: ignore

    def test_repeat_error_event(self) -> None:
        async def demo_start_event() -> None:
            pass

        rap_server: Server = Server(start_event_list=[demo_start_event])
        with pytest.raises(ImportError):
            rap_server.load_start_event([demo_start_event])


class TestServerMiddleware:
    def test_load_error_middleware(self) -> None:
        with pytest.raises(RuntimeError):
            Server(middleware_list=[ServerCryptoProcessor({"test": "keyskeyskeyskeys"}, redis)])  # type: ignore

    def test_repeat_load_middleware(self) -> None:
        access_msg_middleware: AccessMsgMiddleware = AccessMsgMiddleware()
        rap_server: Server = Server(middleware_list=[access_msg_middleware])
        with pytest.raises(ImportError):
            rap_server.load_middleware([access_msg_middleware])


class TestServerProcessor:
    def test_load_error_processor(self) -> None:
        with pytest.raises(RuntimeError):
            Server(processor_list=[AccessMsgMiddleware()])  # type: ignore

    def test_repeat_load_processor(self) -> None:
        crypto_process: ServerCryptoProcessor = ServerCryptoProcessor({"test": "keyskeyskeyskeys"}, redis)
        rap_server: Server = Server(processor_list=[crypto_process])
        with pytest.raises(ImportError):
            rap_server.load_processor([crypto_process])


class TestServerConnHandle:
    async def test_request_handle_error(self, rap_server: Server, rap_client: Client, mocker: MockerFixture) -> None:
        future: asyncio.Future = asyncio.Future()
        future.set_exception(Exception())
        mocker.patch("rap.server.receiver.Receiver.dispatch").return_value = future
        with pytest.raises(ServerError):
            await rap_client.raw_call("sync_sum", [1, 2])

    async def test_receive_none_msg(self, rap_server: Server, rap_client: Client, mocker: MockerFixture) -> None:
        mocker.patch("rap.client.model.Request.gen_request_msg").return_value = None
        with pytest.raises(ConnectionError) as e:
            await rap_client.raw_call("sync_sum", [1, 2])

        exec_msg = e.value.args[0]
        assert exec_msg == "recv close conn event, event info:request is empty"

    async def test_receive_error_msg(self, rap_server: Server, rap_client: Client, mocker: MockerFixture) -> None:
        mocker.patch("rap.server.model.Request.from_msg").side_effect = Exception()
        with pytest.raises(ConnectionError) as e:
            await rap_client.raw_call("sync_sum", [1, 2])

        exec_msg = e.value.args[0]
        assert exec_msg == "recv close conn event, event info:protocol error"

    async def test_read_timeout(self, rap_server: Server, rap_client: Client, mocker: MockerFixture) -> None:
        mock_future: asyncio.Future = asyncio.Future()
        mocker.patch("rap.common.conn.ServerConnection.read").return_value = mock_future
        mock_future.set_exception(asyncio.TimeoutError())

        with pytest.raises(ConnectionError) as e:
            await rap_client.raw_call("sync_sum", [1, 2])

        exec_msg = e.value.args[0]
        assert exec_msg == "recv close conn event, event info:keep alive timeout"


class TestRequestHandle:
    async def test_request_dispatch_not_found(
        self, rap_server: Server, rap_client: Client, mocker: MockerFixture
    ) -> None:
        mocker.patch("rap.client.model.Request.gen_request_msg").return_value = (-1, -1, "default", "", {}, None)
        # self.num, msg_id, self.group, self.func_name, self.header, self.body

        with pytest.raises(ServerError) as e:
            await rap_client.raw_call("sync_sum", [1, 2])

        exec_msg = e.value.args[0]
        assert exec_msg == "Illegal request"

    async def test_request_dispatch_func_error(
        self, rap_server: Server, rap_client: Client, mocker: MockerFixture
    ) -> None:
        mocker.patch("rap.server.receiver.check_func_type").side_effect = Exception()

        with pytest.raises(RpcRunTimeError) as e:
            await rap_client.raw_call("sync_sum", [1, 2])

        exec_msg = e.value.args[0]
        assert exec_msg == "Rpc run time error"
