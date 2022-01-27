import asyncio

import pytest
from aredis import StrictRedis  # type: ignore
from pytest_mock import MockerFixture

from rap.client import Client
from rap.common.asyncio_helper import Deadline
from rap.common.exceptions import RpcRunTimeError
from rap.common.utils import EventEnum
from rap.server import Server
from rap.server.plugin.middleware.conn.limit import ConnLimitMiddleware
from rap.server.plugin.processor import CryptoProcessor as ServerCryptoProcessor
from tests.conftest import AnyStringWith

pytestmark = pytest.mark.asyncio

redis: StrictRedis = StrictRedis.from_url("redis://localhost")


class TestServerEvent:
    def test_load_event_by_init(self) -> None:
        async def demo_event(app: Server) -> None:
            pass

        rap_server: Server = Server("test")
        for key, value in EventEnum.__members__.items():
            rap_server.register_server_event(value, demo_event)
            assert rap_server._server_event_dict[value][-1] == demo_event

    def test_repeat_error_event(self) -> None:
        async def demo_event(app: Server) -> None:
            pass

        for key, value in EventEnum.__members__.items():
            rap_server: Server = Server("test")
            rap_server.register_server_event(value, demo_event)
            with pytest.raises(ImportError):
                rap_server.register_server_event(value, demo_event)


class TestServerMiddleware:
    def test_load_error_middleware(self) -> None:
        with pytest.raises(RuntimeError):
            Server("test", middleware_list=[ServerCryptoProcessor({"test": "keyskeyskeyskeys"}, redis)])  # type: ignore

    def test_repeat_load_middleware(self) -> None:
        conn_limit_middleware: ConnLimitMiddleware = ConnLimitMiddleware()
        rap_server: Server = Server("test", middleware_list=[conn_limit_middleware])
        with pytest.raises(ImportError):
            rap_server.load_middleware([conn_limit_middleware])


class TestServerProcessor:
    def test_load_error_processor(self) -> None:
        with pytest.raises(RuntimeError):
            Server("test", processor_list=[ConnLimitMiddleware()])  # type: ignore

    def test_repeat_load_processor(self) -> None:
        crypto_process: ServerCryptoProcessor = ServerCryptoProcessor({"test": "keyskeyskeyskeys"}, redis)
        rap_server: Server = Server("test", processor_list=[crypto_process])
        with pytest.raises(ImportError):
            rap_server.load_processor([crypto_process])


class TestServerConnHandle:
    async def test_request_handle_error(self, rap_server: Server, rap_client: Client, mocker: MockerFixture) -> None:
        future: asyncio.Future = asyncio.Future()
        future.set_exception(Exception())
        mocker.patch("rap.server.receiver.Receiver.dispatch").return_value = future
        error_log = mocker.patch("rap.client.transport.transport.logger.error")
        with pytest.raises(asyncio.TimeoutError):
            with Deadline(1):
                await rap_client.raw_invoke("sync_sum", [1, 2])
        error_log.assert_called_with(AnyStringWith("Can not dispatch response"))

    # async def test_receive_error_msg(self, rap_server: Server, rap_client: Client, mocker: MockerFixture) -> None:
    #     mocker.patch("rap.server.model.Request.from_msg").side_effect = Exception()
    #     with pytest.raises(ConnectionError) as e:
    #         await rap_client.raw_invoke("sync_sum", [1, 2])
    #
    #     exec_msg = e.value.args[0]
    #     assert exec_msg == "recv close transport event, event info:protocol error"

    # async def test_read_timeout(self, rap_server: Server, rap_client: Client, mocker: MockerFixture) -> None:
    #     with pytest.raises(ConnectionError) as e:
    #         mock_future: asyncio.Future = asyncio.Future()
    #         mocker.patch("rap.common.transport.ServerConnection.read").return_value = mock_future
    #         mock_future.set_exception(asyncio.TimeoutError())
    #         await rap_client.raw_invoke("sync_sum", [1, 2])
    #
    #     exec_msg = e.value.args[0]
    #     assert exec_msg == "recv close transport event, event info:keep alive timeout"


class TestRequestHandle:
    # async def test_request_dispatch_not_found(
    #     self, rap_server: Server, rap_client: Client, mocker: MockerFixture
    # ) -> None:
    #     mocker.patch("rap.client.model.Request.to_msg").return_value = (-1, "123", "/default/test", {}, None)
    #     # self.msg_type, msg_id, self.correlation_id, self.target, self.header, self.body
    #
    #     with pytest.raises(ServerError) as e:
    #         await rap_client.raw_invoke("sync_sum", [1, 2])
    #
    #     exec_msg = e.value.args[0]
    #     assert exec_msg == "Illegal request"

    async def test_request_dispatch_func_error(
        self, rap_server: Server, rap_client: Client, mocker: MockerFixture
    ) -> None:
        mocker.patch("rap.server.receiver.param_handle").side_effect = Exception()

        with pytest.raises(RpcRunTimeError) as e:
            await rap_client.raw_invoke("sync_sum", [1, 2])

        exec_msg = e.value.args[0]
        assert exec_msg == "Rpc run time error"
