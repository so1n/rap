from typing import AsyncIterator

import pytest

from rap.client import Client, Request, Response
from rap.client.processor import BaseProcessor
from rap.common.utils import Constant
from rap.server import Server

from .conftest import async_gen, async_sum, sync_gen

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
            assert 3 == await rap_client.call(async_sum, [1, 2])
            assert 3 == await async_sum(1, 2)

            # async iterator will create session or reuse session
            async for i in async_gen(10):
                print(f"async gen result:{i}")

    async def test_no_create(self, rap_server: Server, rap_client: Client) -> None:

        with pytest.raises(ConnectionError) as e:
            print(rap_client.session.conn)

        exec_msg: str = e.value.args[0]
        assert exec_msg == "Session has not been created"

    async def test_param(self, rap_server: Server, rap_client: Client) -> None:
        rap_client.load_processor([check_session_processor])
        async with rap_client.session as s:
            check_session_processor.session_id = s.id
            assert 3 == await rap_client.call(async_sum, [1, 2], session=s)
            assert 3 == await async_sum(1, 2)

            # async iterator will create session or reuse session
            async for i in sync_gen(10):
                print(f"async gen result:{i}")

    async def test_execute(self, rap_server: Server, rap_client: Client) -> None:
        rap_client.load_processor([check_session_processor])

        def sync_sum(a: int, b: int) -> int:
            return a+ b

        async with rap_client.session as s:
            check_session_processor.session_id = s.id
            # not param rap func
            with pytest.raises(RuntimeError) as e:
                assert 3 == await s.execute(async_sum, arg_list=[1, 2])

            exec_msg: str = e.value.args[0]
            assert exec_msg == "RapFunc has not been called"
            # str
            assert 3 == await s.execute("sync_sum", arg_list=[1, 2])
            # func
            assert 3 == await s.execute(sync_sum, arg_list=[1, 2])
            # rap func
            assert 3 == await s.execute(async_sum(1, 2))
            # Illegal type
            with pytest.raises(TypeError):
                await s.execute(123, arg_list=[1, 2])

    async def test_async_iterator_call(self, rap_server: Server, rap_client: Client) -> None:
        class SessionProcessor(BaseProcessor):
            def __init__(self) -> None:
                self._request_id_set: set = set()
                self._response_id_set: set = set()

            async def process_request(self, request: Request) -> Request:
                if request.num in (Constant.CHANNEL_REQUEST, Constant.MSG_REQUEST):
                    self._request_id_set.add(request.header["session_id"])
                return request

            async def process_response(self, response: Response) -> Response:
                if response.num in (Constant.CHANNEL_RESPONSE, Constant.MSG_RESPONSE):
                    self._response_id_set.add(response.header["session_id"])
                return response

        session_processor: SessionProcessor = SessionProcessor()
        rap_client.load_processor([session_processor])

        async def async_gen(a: int) -> AsyncIterator[int]:
            yield 0

        async for i in rap_client.iterator_call(async_gen, [10]):
            print(f"async gen result:{i}")

        assert len(session_processor._request_id_set) == 1
        assert len(session_processor._response_id_set) == 1
