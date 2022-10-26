from typing import List

import pytest

from rap.client.model import Request, Response
from rap.client.processor.base import BaseClientProcessor, ResponseCallable
from rap.common.utils import constant
from rap.server import Server
from tests.conftest import async_sum, process_client  # type: ignore

pytestmark = pytest.mark.asyncio


class TestProcessor:
    async def test_multi_processor(self, rap_server: Server) -> None:
        process_request_list: List[int] = []
        process_response_list: List[int] = []

        class Processor(BaseClientProcessor):
            def __init__(self, number: int) -> None:
                self._number = number

            async def process_request(self, request: Request) -> Request:
                if request.msg_type == constant.MSG_REQUEST:
                    process_request_list.append(self._number)
                return await super().process_request(request)

            async def process_response(self, response_cb: ResponseCallable) -> Response:
                response: Response = await super().process_response(response_cb)
                if response.msg_type == constant.MSG_RESPONSE:
                    process_response_list.append(self._number)
                return response

        async with process_client(Processor(1), Processor(2), Processor(3)) as rap_client:
            for _ in range(3):
                assert 3 == await rap_client.invoke_by_name("sync_sum", (1, 2))
        assert process_request_list == process_response_list[::-1]
