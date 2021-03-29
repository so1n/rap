import asyncio
import pytest
from typing import Any

from rap.client import Client
from rap.common.conn import BaseConnection
from rap.common.exceptions import RPCError, ServerError
from rap.common.utils import Constant, Event
from rap.server import Server
from rap.server.model import ResponseModel
from rap.server.response import Response

pytestmark = pytest.mark.asyncio
test_exc: Exception = Exception("this is test exc")
test_rpc_exc: RPCError = RPCError("this is rpc error")
test_event: Event = Event(event_name="test", event_info="test event")


class TestServerResponse:
    async def test_set_exc(self) -> None:
        response: ResponseModel = ResponseModel()
        with pytest.raises(TypeError):
            response.set_exception(test_event)

        response.set_exception(test_exc)
        assert response.body == str(test_exc)
        assert response.header["status_code"] == ServerError.status_code

        response = ResponseModel()
        response.set_exception(test_rpc_exc)
        assert response.body == str(test_rpc_exc)
        assert response.header["status_code"] == RPCError.status_code

    async def test_from_exc(self) -> None:
        response: ResponseModel = ResponseModel.from_exc(test_exc)
        assert response.body == str(test_exc)
        assert response.header["status_code"] == ServerError.status_code
        response = ResponseModel.from_exc(test_rpc_exc)
        assert response.body == str(test_rpc_exc)
        assert response.header["status_code"] == RPCError.status_code

        with pytest.raises(TypeError):
            ResponseModel.from_exc(test_event)

    async def test_set_event(self) -> None:
        response: ResponseModel = ResponseModel()
        with pytest.raises(TypeError):
            response.set_event(test_exc)

        response.set_event(test_event)
        assert response.num == Constant.SERVER_EVENT
        assert response.func_name == test_event.event_name
        assert response.body == test_event.event_info

    async def test_from_event(self) -> None:
        response: ResponseModel = ResponseModel.from_event(test_event)
        assert response.num == Constant.SERVER_EVENT
        assert response.func_name == test_event.event_name
        assert response.body == test_event.event_info

        with pytest.raises(TypeError):
            ResponseModel.from_event(test_exc)

    async def test_set_body(self) -> None:
        response: ResponseModel = ResponseModel()
        body: dict = {"a": 1, "b": 2}
        response.set_body(body)
        assert body == response.body

    async def test_call__call__(self) -> None:
        response: ResponseModel = ResponseModel()
        body: dict = {"a": 1, "b": 2}
        response(body)
        assert body == response.body

        response = ResponseModel()
        response(test_event)
        assert response.num == Constant.SERVER_EVENT
        assert response.func_name == test_event.event_name
        assert response.body == test_event.event_info

        response = ResponseModel()
        response(test_rpc_exc)
        assert response.body == str(test_rpc_exc)
        assert response.header["status_code"] == RPCError.status_code

    async def test_response_timeout(self, rap_server: Server, rap_client: Client, mocker: Any) -> None:
        mock_future: asyncio.Future = asyncio.Future()
        mocker.patch("rap.common.conn.BaseConnection.write").return_value = mock_future
        mock_future.set_exception(asyncio.TimeoutError())
        response: Response = Response(BaseConnection(1), 1, processor_list=[])

        response_model: ResponseModel = ResponseModel()
        response_model.set_body({"a": 1, "b": 2})

        with pytest.raises(asyncio.TimeoutError):
            await response(response_model)
