from typing import List, Tuple

import pytest

from rap.client.model import Request, Response
from rap.client.processor.base import (
    BaseClientProcessor,
    ClientContext,
    ContextExitCallable,
    ContextExitType,
    ResponseCallable,
)
from rap.common.exceptions import FuncNotFoundError
from rap.common.utils import constant
from rap.server import Server
from tests.conftest import async_sum, process_client  # type: ignore

pytestmark = pytest.mark.asyncio


class TestClientProcessor:
    async def test_multi_processor_sort(self, rap_server: Server) -> None:
        context_enter_list: List[int] = []
        context_exit_list: List[int] = []
        process_request_list: List[int] = []
        process_response_list: List[int] = []

        class MyProcessor(BaseClientProcessor):
            def __init__(self, number: int) -> None:
                self._number = number

            async def on_context_enter(self, context: ClientContext) -> ClientContext:
                context_enter_list.append(self._number)
                return await super().on_context_enter(context)

            async def process_request(self, request: Request) -> Request:
                if request.msg_type == constant.MSG_REQUEST:
                    process_request_list.append(self._number)
                return await super().process_request(request)

            async def process_response(self, response_cb: ResponseCallable) -> Response:
                response: Response = await super().process_response(response_cb)
                if response.msg_type == constant.MSG_RESPONSE:
                    process_response_list.append(self._number)
                return response

            async def on_context_exit(self, context_exit_cb: ContextExitCallable) -> ContextExitType:
                context, exc_type, exc_val, exc_tb = await super().on_context_exit(context_exit_cb)
                context_exit_list.append(self._number)
                return context, exc_type, exc_val, exc_tb

        async with process_client(MyProcessor(1), MyProcessor(2), MyProcessor(3)) as rap_client:
            for _ in range(3):
                assert 3 == await rap_client.invoke_by_name("sync_sum", {"a": 1, "b": 2})
        assert process_request_list == process_response_list[::-1]
        assert context_enter_list == context_exit_list[::-1]

    async def test_processor_request_error(self, rap_server: Server) -> None:
        """
        Test `process_request` stage throws an exception

        When an exception occurs in `process_request`, the following situations will occur:
            1. Will not continue to call the `process_request` of the next processor
            2. The `process_response` of the processor will not be called (all processor)
            3. The `context_exit` of each processor will receive an exception thrown by `process_request`
        """
        context_enter_list: List[int] = []
        context_exit_list: List[int] = []
        process_request_list: List[int] = []
        process_response_list: List[int] = []
        context_exit_container_list: List[Tuple] = []

        class MyProcessor(BaseClientProcessor):
            def __init__(self, number: int) -> None:
                self._number = number

            async def on_context_enter(self, context: ClientContext) -> ClientContext:
                context_enter_list.append(self._number)
                return await super().on_context_enter(context)

            async def process_request(self, request: Request) -> Request:
                if request.msg_type == constant.MSG_REQUEST:
                    process_request_list.append(self._number)
                    if self._number == 2 and request.func_name == "sync_sum":
                        raise RuntimeError("Test Error")
                return await super().process_request(request)

            async def process_response(self, response_cb: ResponseCallable) -> Response:
                response: Response = await super().process_response(response_cb)
                if response.msg_type == constant.MSG_RESPONSE:
                    process_response_list.append(self._number)
                return response

            async def on_context_exit(self, context_exit_cb: ContextExitCallable) -> ContextExitType:
                context, exc_type, exc_val, exc_tb = await super().on_context_exit(context_exit_cb)
                context_exit_list.append(self._number)
                if context.target.endswith("sync_sum"):
                    context_exit_container_list.append((exc_type, exc_val))
                return context, exc_type, exc_val, exc_tb

        async with process_client(MyProcessor(1), MyProcessor(2), MyProcessor(3)) as rap_client:
            with pytest.raises(RuntimeError):
                assert 3 == await rap_client.invoke_by_name("sync_sum", {"a": 1, "b": 2})

        assert process_request_list == [1, 2]
        assert process_response_list == []
        assert context_enter_list == context_exit_list[::-1]
        assert len(context_exit_container_list) == 3

        for container in context_exit_container_list:
            assert container[0] == RuntimeError
            assert str(container[1]) == "Test Error"

    async def test_processor_response_error(self, rap_server: Server) -> None:
        """
        Test `process_response` stage throws an exception

        When an exception occurs in `process_response`, the following situations will occur:
            1. Will not continue to call the `process_response` of the prev processor
            2. The `context_exit` of each processor will receive an exception thrown by `process_response`
        """
        context_enter_list: List[int] = []
        context_exit_list: List[int] = []
        process_request_list: List[int] = []
        process_response_list: List[int] = []
        context_exit_container_list: List[Tuple] = []

        class MyProcessor(BaseClientProcessor):
            def __init__(self, number: int) -> None:
                self._number = number

            async def on_context_enter(self, context: ClientContext) -> ClientContext:
                context_enter_list.append(self._number)
                return await super().on_context_enter(context)

            async def process_request(self, request: Request) -> Request:
                if request.msg_type == constant.MSG_REQUEST:
                    process_request_list.append(self._number)
                return await super().process_request(request)

            async def process_response(self, response_cb: ResponseCallable) -> Response:
                response: Response = await super().process_response(response_cb)
                if response.msg_type == constant.MSG_RESPONSE:
                    process_response_list.append(self._number)
                    if self._number == 2 and response.func_name == "sync_sum":
                        raise RuntimeError("Test Error")
                return response

            async def on_context_exit(self, context_exit_cb: ContextExitCallable) -> ContextExitType:
                context, exc_type, exc_val, exc_tb = await super().on_context_exit(context_exit_cb)
                context_exit_list.append(self._number)
                if context.func_name == "sync_sum":
                    context_exit_container_list.append((exc_type, exc_val))
                return context, exc_type, exc_val, exc_tb

        async with process_client(MyProcessor(1), MyProcessor(2), MyProcessor(3)) as rap_client:
            with pytest.raises(RuntimeError):
                assert 3 == await rap_client.invoke_by_name("sync_sum", {"a": 1, "b": 2})

        assert process_request_list == [1, 2, 3]
        assert process_response_list == [3, 2]
        assert context_enter_list == context_exit_list[::-1]
        assert len(context_exit_container_list) == 3

        for container in context_exit_container_list:
            assert container[0] == RuntimeError
            assert str(container[1]) == "Test Error"

    async def test_processor_and_invoke_error(self, rap_server: Server) -> None:
        """
        Test the error returned by the server

        The following will occur:
            1. `process_request` and `on context_enter` of all processors will be executed normally.
            2. `process_response` and `on context_exit` of all processors will pass exceptions
        """
        context_enter_list: List[int] = []
        context_exit_list: List[int] = []
        process_request_list: List[int] = []
        process_response_list: List[int] = []
        context_exit_container_list: List[Tuple] = []
        process_response_container_list: List[Exception] = []

        class MyProcessor(BaseClientProcessor):
            def __init__(self, number: int) -> None:
                self._number = number

            async def on_context_enter(self, context: ClientContext) -> ClientContext:
                context_enter_list.append(self._number)
                return await super().on_context_enter(context)

            async def process_request(self, request: Request) -> Request:
                if request.msg_type == constant.MSG_REQUEST:
                    process_request_list.append(self._number)
                return await super().process_request(request)

            async def process_response(self, response_cb: ResponseCallable) -> Response:
                try:
                    response: Response = await super().process_response(response_cb)
                    if response and response.msg_type == constant.MSG_RESPONSE:
                        process_response_list.append(self._number)
                    return response
                except Exception as e:
                    # Re-acquire the response object of this response, while avoiding exception throwing
                    response, _e = await response_cb(False)
                    # Since it is an error returned by the server, they are consistent
                    assert e == _e
                    process_response_container_list.append(e)
                    raise e

            async def on_context_exit(self, context_exit_cb: ContextExitCallable) -> ContextExitType:
                context, exc_type, exc_val, exc_tb = await super().on_context_exit(context_exit_cb)
                context_exit_list.append(self._number)
                if context.func_name == "error_func_name":
                    context_exit_container_list.append((exc_type, exc_val))
                return context, exc_type, exc_val, exc_tb

        async with process_client(MyProcessor(1), MyProcessor(2), MyProcessor(3)) as rap_client:
            with pytest.raises(FuncNotFoundError):
                await rap_client.invoke_by_name("error_func_name", {"a": 1, "b": 2})

        assert process_request_list == [1, 2, 3]
        assert process_response_list == []
        assert context_enter_list == context_exit_list[::-1]

        assert len(context_exit_container_list) == 3
        assert len(process_response_container_list) == 3

        for container in context_exit_container_list:
            assert container[0] == FuncNotFoundError
            assert str(container[1]) == "Not found func. name: error_func_name"

        for container in process_response_container_list:
            assert str(container) == "Not found func. name: error_func_name"
