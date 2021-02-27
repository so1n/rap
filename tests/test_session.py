import pytest
from rap.client import Client, Request, Response
from rap.client.processor import BaseProcessor
from rap.server import Server
from rap.common.utlis import Constant
from .conftest import async_sum, async_gen


pytestmark = pytest.mark.asyncio


class CheckSessionProcessor(BaseProcessor):
    def __init__(self) -> None:
        self.session_id: str = ""

    async def process_request(self, request: Request) -> Request:
        if request.num in (Constant.CHANNEL_REQUEST, Constant.MSG_REQUEST):
            assert self.session_id == request.header["session_id"]
        return request

    async def process_response(self, response: Response) -> Response:
        if response.num in (Constant.CHANNEL_RESPONSE, Constant.MSG_RESPONSE):
            assert self.session_id == response.header["session_id"]
        return response


check_session_processor: CheckSessionProcessor = CheckSessionProcessor()


class TestSession:
    async def test_no_param(self, rap_server: Server, rap_client: Client) -> None:
        rap_client.load_processor([check_session_processor])
        async with rap_client.session as s:
            check_session_processor.session_id = s.id
            assert 3 == await rap_client.call(async_sum, 1, 2)
            assert 3 == await async_sum(1, 2)

            # async iterator will create session or reuse session
            async for i in async_gen(10):
                print(f"async gen result:{i}")

    async def test_param(self, rap_server: Server, rap_client: Client) -> None:
        rap_client.load_processor([check_session_processor])
        async with rap_client.session as s:
            check_session_processor.session_id = s.id
            assert 3 == await rap_client.call(async_sum, 1, 2, session=s)
            assert 3 == await async_sum(1, 2)

            # async iterator will create session or reuse session
            async for i in async_gen(10):
                print(f"async gen result:{i}")

    async def test_execute(self, rap_server: Server, rap_client: Client) -> None:
        rap_client.load_processor([check_session_processor])
        async with rap_client.session as s:
            check_session_processor.session_id = s.id
            assert 3 == await s.execute(async_sum, arg_list=[1, 2])
            assert 3 == await s.execute("sync_sum", arg_list=[1, 2])
            assert 3 == await s.execute(async_sum(1, 2))