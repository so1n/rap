import asyncio
from typing import Any

import pytest

from rap.client.transport.transport import Transport
from rap.common.asyncio_helper import Deadline
from rap.server import Server

pytestmark = pytest.mark.asyncio


class TestClientPingPong:
    async def test_ping_pong(self, rap_server: Server) -> None:
        transport: Transport = Transport(host="127.0.0.1", port=9000, weight=10)
        await transport.connect()
        await transport.declare()
        assert transport.pick_score == 10.0

        await transport.ping()
        assert transport.pick_score > 0

    async def test_ping_pong_timeout(self, rap_server: Server) -> None:
        # until close

        transport: Transport = Transport(host="127.0.0.1", port=9000, weight=10)
        await transport.connect()
        await transport.declare()
        write_func = transport.write_to_conn

        async def mock_write(*args: Any, **kwargs: Any) -> Any:
            if not args[0].target.endswith("ping"):
                return await write_func(*args, **kwargs)

        setattr(transport, "write_to_conn", mock_write)

        with pytest.raises(asyncio.TimeoutError):
            with Deadline(delay=0.5):
                await transport.ping(cnt=1)
        setattr(transport, "write_to_conn", write_func)
