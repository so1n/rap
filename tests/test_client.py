import asyncio
from typing import Any

import pytest
from pytest_mock import MockerFixture

from rap.client import Client, Request, Response
from rap.common.exceptions import ChannelError, RPCError
from rap.common.utils import Constant
from rap.server import Server

pytestmark = pytest.mark.asyncio


class AnyStringWith(str):
    def __eq__(self, other: Any) -> bool:
        return self in other


async def mock_func(self: Any) -> None:
    await asyncio.sleep(0)


class TestClient:
    async def test_client_repeat_conn(self, rap_server: Server, rap_client: Client) -> None:
        with pytest.raises(ConnectionError) as e:
            await rap_client.start()

        exec_msg = e.value.args[0]
        assert exec_msg == "Transport is running"


class TestTransport:
    @staticmethod
    async def _read_helper(mocker: MockerFixture, once_target: Any) -> None:
        mocker_obj: Any = mocker.patch("rap.client.transport.transport.logging.error")
        client: Client = Client()
        client.add_conn("localhost", 9000)
        setattr(client.transport, "_listen", mock_func)
        await client.start()

        for conn_model in client.transport._conn_dict.values():
            await client.transport._read_from_conn(conn_model.conn)

        mocker_obj.assert_called_once_with(once_target)

    async def test_read_conn_timeout(self, rap_server: Server, rap_client: Client, mocker: MockerFixture) -> None:
        mock_future: asyncio.Future = asyncio.Future()
        mocker.patch("rap.common.conn.Connection.read").return_value = mock_future
        mock_future.set_exception(asyncio.TimeoutError())

        with pytest.raises(asyncio.TimeoutError):
            await rap_client.raw_call("sync_sum", [1, 2])

    async def test_write_timeout(self, rap_server: Server, rap_client: Client, mocker: MockerFixture) -> None:
        mock_future: asyncio.Future = asyncio.Future()
        mocker.patch("rap.common.conn.Connection.write").return_value = mock_future
        mock_future.set_exception(asyncio.TimeoutError())

        with pytest.raises(asyncio.TimeoutError):
            await rap_client.raw_call("sync_sum", [1, 2])

    async def test_write_raise_exc(self, rap_server: Server, rap_client: Client, mocker: MockerFixture) -> None:
        mock_future: asyncio.Future = asyncio.Future()
        mocker.patch("rap.common.conn.Connection.write").return_value = mock_future
        mock_future.set_exception(Exception("demo"))

        with pytest.raises(Exception) as e:
            await rap_client.raw_call("sync_sum", [1, 2])

        exec_msg: str = e.value.args[0]
        assert exec_msg == "demo"

    async def test_read_timeout(self, rap_server: Server, rap_client: Client, mocker: MockerFixture) -> None:
        mock_future: asyncio.Future = asyncio.Future()
        mocker.patch("rap.client.transport.transport.as_first_completed").return_value = mock_future
        mock_future.set_exception(asyncio.TimeoutError())

        with pytest.raises(asyncio.TimeoutError) as e:
            await rap_client.raw_call("sync_sum", [1, 2])

        exec_msg: str = e.value.args[0]
        assert exec_msg.endswith("request timeout")

    async def test_read_none_msg(self, rap_server: Server, rap_client: Client, mocker: MockerFixture) -> None:
        mock_future: asyncio.Future = asyncio.Future()
        mocker.patch("rap.common.conn.Connection.read").return_value = mock_future
        mock_future.set_result(None)

        with pytest.raises(ConnectionError) as e:
            await rap_client.raw_call("sync_sum", [1, 2])

        exec_msg: str = e.value.args[0]
        assert exec_msg == "Connection has been closed"

    async def test_read_error_msg(self, rap_server: Server, mocker: MockerFixture) -> None:
        mock_future: asyncio.Future = asyncio.Future()
        mocker.patch("rap.common.conn.Connection.read").return_value = mock_future
        response_msg: tuple = (1, 2, 3, 4, 5)
        mock_future.set_result(response_msg)

        await self._read_helper(mocker, AnyStringWith(f"recv wrong response:{response_msg}, ignore"))

    async def test_read_error_event(self, rap_server: Server, mocker: MockerFixture) -> None:
        mock_future: asyncio.Future = asyncio.Future()
        mocker.patch("rap.common.conn.Connection.read").return_value = mock_future
        mock_future.set_result(
            (
                203,
                -1,
                "default",
                "facker",
                {
                    "status_code": 200,
                    "version": "0.1",
                    "user_agent": "Python3-0.5.3",
                    "request_id": "cf172603-5783-4b0c-92b1-62667626e9d0",
                },
                "",
            )
        )

        await self._read_helper(mocker, AnyStringWith("recv error event"))

    async def test_read_not_found_channel_id(self, rap_server: Server, mocker: MockerFixture) -> None:
        mock_future: asyncio.Future = asyncio.Future()
        mocker.patch("rap.common.conn.Connection.read").return_value = mock_future
        channel_id: str = "faker_id"
        mock_future.set_result(
            (
                202,
                -1,
                "default",
                "faker",
                {
                    "channel_life_cycle": "MSG",
                    "channel_id": channel_id,
                    "version": "0.1",
                    "user_agent": "Python3-0.5.3",
                    "request_id": "57233e1f-b153-4142-b278-29c755394394",
                    "status_code": 200,
                },
                "hi!",
            )
        )

        await self._read_helper(mocker, f"recv {channel_id} msg, but {channel_id} not create")

    async def test_read_not_parse_response(self, rap_server: Server, mocker: MockerFixture) -> None:
        mock_future: asyncio.Future = asyncio.Future()
        mocker.patch("rap.common.conn.Connection.read").return_value = mock_future
        mock_future.set_result(
            (
                202,
                -1,
                "default",
                "faker",
                {
                    "channel_life_cycle": "MSG",
                    "version": "0.1",
                    "user_agent": "Python3-0.5.3",
                    "request_id": "57233e1f-b153-4142-b278-29c755394394",
                    "status_code": 200,
                },
                "hi!",
            )
        )

        await self._read_helper(mocker, AnyStringWith("Can' parse response:"))

    async def test_request_receive_error_response_num(
        self, rap_server: Server, rap_client: Client, mocker: MockerFixture
    ) -> None:
        mock_future: asyncio.Future = asyncio.Future()
        mocker.patch("rap.client.transport.transport.Transport._base_request").return_value = mock_future
        mock_future.set_result(
            Response.from_msg(
                (
                    202,
                    -1,
                    "default",
                    "faker",
                    {
                        "version": "0.1",
                        "user_agent": "Python3-0.5.3",
                        "request_id": "57233e1f-b153-4142-b278-29c755394394",
                        "status_code": 200,
                    },
                    "hi!",
                )
            )
        )

        with pytest.raises(RPCError) as e:
            await rap_client.raw_call("sync_sum", [1, 2])

        exec_msg: str = e.value.args[0]
        assert exec_msg == f"request num must:{Constant.MSG_RESPONSE} not 202"

    async def test_request_receive_not_python_server_exc_response(
        self, rap_server: Server, rap_client: Client, mocker: MockerFixture
    ) -> None:
        def raise_msg_exc(a: int, b: int) -> int:
            return int(1 / 0)

        rap_server.register(raise_msg_exc)

        mock_future: asyncio.Future = asyncio.Future()
        mocker.patch("rap.client.transport.transport.Transport._base_request").return_value = mock_future
        mock_future.set_result(
            Response.from_msg(
                (
                    201,
                    29759,
                    "default",
                    "raise_msg_exc",
                    {
                        "status_code": 200,
                        "version": "0.1",
                        "request_id": "fe41e811-3cd0-45e7-b83a-738759cb0ad8",
                        "host": ("127.0.0.1", 59022),
                    },
                    {"call_id": -1, "exc_info": "division by zero", "exc": "ZeroDivisionError"},
                )
            )
        )

        with pytest.raises(RuntimeError) as e:
            await rap_client.raw_call("sync_sum", [1, 2])

        exec_msg: str = e.value.args[0]
        assert exec_msg == "division by zero"

    async def test_write_channel_msg_not_channel_id(self, rap_server: Server, rap_client: Client) -> None:
        with pytest.raises(ChannelError) as e:
            async with rap_client.session as s:
                await rap_client.transport.write(
                    Request(Constant.CHANNEL_REQUEST, "test", None, "default"), -1, session=s
                )

        exec_msg: str = e.value.args[0]
        assert exec_msg == "not found channel id in header"
