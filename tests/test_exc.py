import pytest

from rap.client import Client
from rap.common.exceptions import FuncNotFoundError
from rap.server import Server

pytestmark = pytest.mark.asyncio


class TestExc:
    async def test_raise_msg_exc(self, rap_server: Server, rap_client: Client) -> None:
        @rap_client.register()
        async def raise_msg_exc(a: int, b: int) -> int:
            return 0

        def _raise_msg_exc(a: int, b: int) -> int:
            return int(1 / 0)

        rap_server.register(_raise_msg_exc, "raise_msg_exc")
        with pytest.raises(ZeroDivisionError):
            await raise_msg_exc(1, 2)

    async def test_raise_server_not_found_func_exc(self, rap_server: Server, rap_client: Client) -> None:
        @rap_client.register()
        async def raise_server_not_found_func_exc(a: int) -> None:
            pass

        with pytest.raises(FuncNotFoundError):
            await raise_server_not_found_func_exc(1)
