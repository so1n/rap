import pytest

from rap.common.exceptions import RPCError, ServerError
from rap.common.utlis import Constant, Event
from rap.server.model import ResponseModel


pytestmark = pytest.mark.asyncio
test_exc: Exception = Exception('this is test exc')
test_rpc_exc: RPCError = RPCError('this is rpc error')
test_event: Event = Event(event_name='test', event_info='test event')


class TestServerResponse:
    async def test_set_exc(self) -> None:
        response: ResponseModel = ResponseModel()
        with pytest.raises(TypeError):
            response.set_exception(test_event)

        response.set_exception(test_exc)
        assert response.body == str(test_exc)
        assert response.header['status_code'] == ServerError.status_code

        response = ResponseModel()
        response.set_exception(test_rpc_exc)
        assert response.body == str(test_rpc_exc)
        assert response.header['status_code'] == RPCError.status_code

    async def test_from_exc(self) -> None:
        response: ResponseModel = ResponseModel.from_exc(test_exc)
        assert response.body == str(test_exc)
        assert response.header['status_code'] == ServerError.status_code
        response = ResponseModel.from_exc(test_rpc_exc)
        assert response.body == str(test_rpc_exc)
        assert response.header['status_code'] == RPCError.status_code

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
        body: dict = {'a': 1, 'b': 2}
        response.set_body(body)
        assert body == response.body

    async def test_call__call__(self) -> None:
        response: ResponseModel = ResponseModel()
        body: dict = {'a': 1, 'b': 2}
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
        assert response.header['status_code'] == RPCError.status_code
