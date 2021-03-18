import pytest
from aredis import StrictRedis

from rap.server import Server
from rap.server.middleware.msg.access import AccessMsgMiddleware
from rap.server.processor import CryptoProcessor as ServerCryptoProcessor

from .conftest import async_sum

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
            Server(start_event_list=["111"])

    def test_repeat_error_event(self) -> None:
        async def demo_start_event() -> None:
            pass

        rap_server: Server = Server(start_event_list=[demo_start_event])
        with pytest.raises(ImportError):
            rap_server.load_start_event([demo_start_event])

    def test_load_error_middleware(self) -> None:
        with pytest.raises(RuntimeError):
            Server(middleware_list=[ServerCryptoProcessor({"test": "keyskeyskeyskeys"}, redis)])

    def test_repeat_load_middleware(self) -> None:
        access_msg_middleware: AccessMsgMiddleware = AccessMsgMiddleware()
        rap_server: Server = Server(middleware_list=[access_msg_middleware])
        with pytest.raises(ImportError):
            rap_server.load_middleware([access_msg_middleware])

    def test_load_error_processor(self) -> None:
        with pytest.raises(RuntimeError):
            Server(processor_list=[AccessMsgMiddleware()])

    def test_repeat_load_processor(self) -> None:
        crypto_process: ServerCryptoProcessor = ServerCryptoProcessor({"test": "keyskeyskeyskeys"}, redis)
        rap_server: Server = Server(processor_list=[crypto_process])
        with pytest.raises(ImportError):
            rap_server.load_processor([crypto_process])
