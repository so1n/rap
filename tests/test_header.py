import pytest

from rap.client.transport.transport import Transport
from rap.common.channel import UserChannel
from rap.common.event import DeclareEvent
from rap.common.utils import constant
from rap.server.model import Request, Response
from rap.server.plugin.processor.base import BaseProcessor, ResponseCallable
from tests.conftest import load_processor, process_server

pytestmark = pytest.mark.asyncio


class TestHeader:
    async def test_transport_header(self) -> None:
        header: dict = {"test": "test transport header"}
        recv_header: dict = {}
        response_recv_header: dict = {}

        class MyProcessor(BaseProcessor):
            async def process_request(self, request: Request) -> Request:
                if request.func_name.endswith(DeclareEvent.event_name):
                    nonlocal recv_header
                    recv_header = request.body["metadata"]
                return request

            async def process_response(self, response_cb: ResponseCallable) -> Response:
                response: Response = await super().process_response(response_cb)
                if response.target.endswith(DeclareEvent.event_name):
                    nonlocal response_recv_header
                    response_recv_header = response.context.transport_metadata
                return response

        async with process_server([MyProcessor()]):
            transport: Transport = Transport(host="127.0.0.1", port=9000, weight=10, metadata=header)
            assert transport.metadata == header
            await transport.connect()
            await transport.declare()
        assert recv_header == header
        assert response_recv_header == header

    async def test_channel_header(self) -> None:
        request_recv_header: dict = {}
        response_recv_header: dict = {}
        header: dict = {"test": "test channel header"}

        class MyProcessor(BaseProcessor):
            async def process_request(self, request: Request) -> Request:
                if (
                    request.msg_type is constant.CHANNEL_REQUEST
                    and request.header["channel_life_cycle"] == constant.DECLARE
                ):
                    nonlocal request_recv_header
                    request_recv_header = request.body
                return request

            async def process_response(self, response_cb: ResponseCallable) -> Response:
                response: Response = await super().process_response(response_cb)
                if (
                    response.msg_type is constant.CHANNEL_RESPONSE
                    and response.header["channel_life_cycle"] == constant.DECLARE
                ):
                    nonlocal response_recv_header
                    response_recv_header = response.context.channel_metadata
                return response

        async with load_processor([MyProcessor()], []) as c_and_s:
            client, server = c_and_s

            async def test_channel(channel: UserChannel) -> None:
                return

            await client.invoke_channel(test_channel, metadata=header)()

        assert request_recv_header == header
        assert response_recv_header == header
