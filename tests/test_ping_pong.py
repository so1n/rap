import asyncio
from typing import Any

import pytest
from pytest_mock import MockerFixture

from rap.client import Client
from rap.client.transport.transport import Transport
from rap.common.asyncio_helper import Deadline
from rap.server import Server

pytestmark = pytest.mark.asyncio


class TestPingPong:
    async def test_ping_pong(self, rap_server: Server, rap_client: Client) -> None:
        await asyncio.sleep(10)
        # for key, transport_group in rap_client.endpoint._transport_pool_dict.copy().items():
        #     await transport_group.transport.ping(cnt=1)

    async def test_ping_pong_timeout(self, mocker: MockerFixture, rap_server: Server, rap_client: Client) -> None:
        # until close
        for key, transport_group in rap_client.endpoint._transport_pool_dict.copy().items():
            transport: Transport = (await transport_group.use_transport()).transport
            write_func = transport.write_to_conn

            async def mock_write(*args: Any, **kwargs: Any) -> Any:
                if not args[0].target.endswith("ping"):
                    return await write_func(*args, **kwargs)

            setattr(transport, "write_to_conn", mock_write)

            with pytest.raises(asyncio.TimeoutError):
                with Deadline(delay=0.5):
                    transport: Transport = (await transport_group.use_transport()).transport
                    await transport.ping(cnt=1)
            setattr(transport, "write_to_conn", write_func)
