import asyncio
import pytest
from typing import Any

from rap.client import Client
from rap.server import Server

pytestmark = pytest.mark.asyncio


class TestClient:

    async def test_write_timeout(self, rap_server: Server, rap_client: Client, mocker: Any) -> None:
        mock_future: asyncio.Future = asyncio.Future()
        mocker.patch("rap.common.conn.BaseConnection.write").return_value = mock_future
        mock_future.set_exception(asyncio.TimeoutError())

        with pytest.raises(asyncio.TimeoutError):
            await rap_client.raw_call("sync_sum", [1, 2])
