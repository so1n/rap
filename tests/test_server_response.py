import asyncio
from typing import Any

import pytest

from rap.client import Client
from rap.common.conn import BaseConnection
from rap.common.event import Event
from rap.common.exceptions import RPCError, ServerError
from rap.common.utils import constant
from rap.server import Server
from rap.server.model import Response, ServerContext
from rap.server.sender import Sender

pytestmark = pytest.mark.asyncio
test_exc: Exception = Exception("this is test exc")
test_rpc_exc: RPCError = RPCError("this is rpc error")
test_event: Event = Event(event_info="test event", event_name="test")
test_target: str = f"/{constant.DEFAULT_GROUP}/test"


class TestServerResponse:
    async def test_set_exc(self) -> None:
        target: str = "/_exc/exc"
        response: Response = Response(context=ServerContext(), target=target)
        with pytest.raises(AssertionError):
            response.set_exception(test_event)  # type: ignore

        response.set_exception(test_exc)
        assert response.body == str(test_exc)
        assert response.status_code == ServerError.status_code

        response = Response(context=ServerContext(), target=target)
        response.set_exception(test_rpc_exc)
        assert response.body == str(test_rpc_exc)
        assert response.status_code == RPCError.status_code

    async def test_from_exc(self) -> None:
        response: Response = Response.from_exc(test_exc, ServerContext())
        assert response.body == str(test_exc)
        assert response.status_code == ServerError.status_code
        response = Response.from_exc(test_rpc_exc, ServerContext())
        assert response.body == str(test_rpc_exc)
        assert response.status_code == RPCError.status_code

        with pytest.raises(AssertionError):
            Response.from_exc(test_event, ServerContext())  # type: ignore

    async def test_set_event(self) -> None:
        target: str = "/_event/event"
        response: Response = Response(context=ServerContext(), target=target, msg_type=constant.CLIENT_EVENT)
        with pytest.raises(AssertionError):
            response.set_event(test_exc)  # type: ignore

        response.set_event(test_event)
        assert response.msg_type == constant.CLIENT_EVENT
        assert response.target == target
        assert response.body == test_event.event_info

    async def test_set_server_event(self) -> None:
        target: str = "/_event/event"
        response: Response = Response(context=ServerContext(), target=target)
        with pytest.raises(AssertionError):
            response.set_server_event(test_exc)  # type: ignore

        response.set_server_event(test_event)
        assert response.msg_type == constant.SERVER_EVENT
        assert response.target.endswith(test_event.event_name)
        assert response.body == test_event.event_info

    async def test_from_event(self) -> None:
        response: Response = Response.from_event(test_event, context=ServerContext())
        assert response.msg_type == constant.SERVER_EVENT
        assert response.target.endswith(test_event.event_name)
        assert response.body == test_event.event_info

        with pytest.raises(AssertionError):
            Response.from_event(test_exc, ServerContext())  # type: ignore

    async def test_set_body(self) -> None:
        response: Response = Response(target=test_target, context=ServerContext())
        body: dict = {"a": 1, "b": 2}
        response.set_body(body)
        assert body == response.body

    async def test_call__call__(self) -> None:
        response: Response = Response(target=test_target, context=ServerContext())
        body: dict = {"a": 1, "b": 2}
        response(body)
        assert body == response.body

        response = Response(target=test_target, context=ServerContext())
        response(test_event)
        assert response.msg_type == constant.SERVER_EVENT
        assert response.target.endswith(test_event.event_name)
        assert response.body == test_event.event_info

        response = Response(target=test_target, context=ServerContext())
        response(test_rpc_exc)
        assert response.body == str(test_rpc_exc)
        assert response.status_code == RPCError.status_code

    async def test_response_timeout(self, rap_server: Server, rap_client: Client, mocker: Any) -> None:
        mock_future: asyncio.Future = asyncio.Future()
        mocker.patch("rap.common.conn.BaseConnection.write").return_value = mock_future
        mock_future.set_exception(asyncio.TimeoutError())
        response: Sender = Sender(rap_server, BaseConnection(1), 1)  # type: ignore
        response_model: Response = Response(target=test_target, context=response._create_context())
        response_model.set_body({"a": 1, "b": 2})

        with pytest.raises(asyncio.TimeoutError):
            await response(response_model)
