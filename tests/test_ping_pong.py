import asyncio
from typing import Any

import pytest
from pytest_mock import MockerFixture

from rap.client import Client
from rap.common.asyncio_helper import Deadline
from rap.server import Server

pytestmark = pytest.mark.asyncio


class TestPingPong:
    async def test_ping_pong(self, rap_server: Server, rap_client: Client) -> None:
        for conn in rap_client.endpoint._conn_list.copy():
            await rap_client.transport.ping(conn, cnt=1)

    async def test_ping_pong_timeout(self, mocker: MockerFixture, rap_server: Server, rap_client: Client) -> None:
        rap_client_write = rap_client.transport.write_to_conn

        async def mock_write(*args: Any, **kwargs: Any) -> Any:
            if not args[0].target.endswith("ping"):
                return await rap_client_write(*args, **kwargs)

        setattr(rap_client.transport, "write_to_conn", mock_write)

        # until close
        for conn in rap_client.endpoint._conn_list.copy():
            with pytest.raises(asyncio.TimeoutError):
                with Deadline(delay=0.5):
                    await rap_client.transport.ping(conn, cnt=1)

        setattr(rap_client.transport, "write_to_conn", rap_client_write)
