import asyncio
from typing import Any

import pytest
from pytest_mock import MockerFixture

from rap.client import Client
from rap.server import Server

from .conftest import AnyStringWith

pytestmark = pytest.mark.asyncio


class TestPingPong:
    async def test_ping_pong(self, rap_server: Server, rap_client: Client) -> None:
        future: asyncio.Future = asyncio.Future()
        rap_client_write = rap_client.transport.write_to_conn

        async def mock_write(*args: Any, **kwargs: Any) -> Any:
            if args[0].func_name == "pong":
                future.set_result(True)
            return await rap_client_write(*args, **kwargs)

        setattr(rap_client.transport, "write_to_conn", mock_write)
        assert True is await future
        setattr(rap_client.transport, "write_to_conn", rap_client_write)

    async def test_ping_pong_timeout(self, mocker: MockerFixture, rap_server: Server, rap_client: Client) -> None:
        rap_client_write = rap_client.transport.write_to_conn

        async def mock_write(*args: Any, **kwargs: Any) -> Any:
            if args[0].func_name != "pong":
                return await rap_client_write(*args, **kwargs)

        setattr(rap_client.transport, "write_to_conn", mock_write)

        mocker_obj: Any = mocker.patch("rap.client.transport.transport.logging.exception")
        # until close
        for conn in rap_client._endpoint._conn_dict.copy().values():
            try:
                await conn.listen_future
            except asyncio.CancelledError:
                # listen close event, listen will close and raise exc to conn
                pass
            mocker_obj.assert_called_once_with(AnyStringWith("recv close conn event"))

        setattr(rap_client.transport, "write_to_conn", rap_client_write)
