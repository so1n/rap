import asyncio
from typing import AsyncGenerator, Tuple

import pytest
from aredis import StrictRedis  # type: ignore

from rap.client import Client
from rap.server import Server
from rap.server.plugin.processor.statsd import StatsdClient, StatsdProcessor
from tests.conftest import async_sum  # type: ignore

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def udp_server() -> AsyncGenerator[asyncio.Queue, None]:
    result_queue: asyncio.Queue = asyncio.Queue()

    class ServerProtocol(asyncio.DatagramProtocol):
        def datagram_received(self, data: bytes, addr: Tuple[str, int]) -> None:
            result_queue.put_nowait(data)

    loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()

    transport, protocol = await loop.create_datagram_endpoint(lambda: ServerProtocol(), local_addr=("localhost", 8125))
    yield result_queue

    transport.close()


class TestStatsd:
    async def test_statsd(self, rap_server: Server, rap_client: Client, udp_server: asyncio.Queue) -> None:
        statsd_client: StatsdClient = StatsdClient(host="localhost", port=8125)
        try:
            await statsd_client.connect()
            rap_server.load_processor([StatsdProcessor(statsd_client=statsd_client)])
            await rap_client.raw_call("sync_sum", arg_param=[1, 2])
        finally:
            await statsd_client.close()
