import asyncio
import time
from typing import Any

import pytest
from pytest_mock import MockerFixture

from rap.client import Client, Response
from rap.common.exceptions import RPCError, RpcRunTimeError
from rap.common.utils import Constant
from rap.server import Server

from .conftest import AnyStringWith

pytestmark = pytest.mark.asyncio


async def mock_func(self: Any) -> None:
    await asyncio.sleep(0)


class TestClient:
    async def test_client_repeat_conn(self, rap_server: Server, rap_client: Client) -> None:
        with pytest.raises(ConnectionError) as e:
            await rap_client.start()

        exec_msg = e.value.args[0]
        assert exec_msg == "LocalEndpoint is running"


class TestTransport:
    @staticmethod
    async def _read_helper(mocker: MockerFixture, request_tuple: tuple, once_target: Any) -> None:
        mocker_obj: Any = mocker.patch("rap.client.transport.transport.logging.error")
        client: Client = Client("test", [{"ip": "localhost", "port": "9000"}])
        setattr(client.transport, "listen", mock_func)
        await client.start()

        mock_future: asyncio.Future = asyncio.Future()
        mocker.patch("rap.common.conn.Connection.read").return_value = mock_future
        mock_future.set_result(request_tuple)

        for conn in client.endpoint._conn_dict.values():
            await client.transport._dispatch_resp_from_conn(conn)

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
        client: Client = Client("test", [{"ip": "localhost", "port": "9000"}])
        with pytest.raises(Exception) as e:
            await client.start()

        exec_msg = e.value.args[0]
        assert f"recv wrong response:{response_msg}, ignore" in exec_msg

    async def test_read_error_event(self, rap_server: Server, mocker: MockerFixture) -> None:
        await self._read_helper(
            mocker,
            (
                -1,
                203,
                f"{str(int(time.time()))}",
                "/_event/default",
                200,
                {
                    "version": "0.1",
                    "user_agent": "Python3-0.5.3",
                    "request_id": "cf172603-5783-4b0c-92b1-62667626e9d0",
                },
                "",
            ),
            AnyStringWith("recv error event"),
        )

    async def test_read_not_found_channel_id(self, rap_server: Server, mocker: MockerFixture) -> None:
        await self._read_helper(
            mocker,
            (
                -1,
                202,
                "faker_id",
                "/default/test_channel",
                200,
                {
                    "channel_life_cycle": "MSG",
                    "version": "0.1",
                    "user_agent": "Python3-0.5.3",
                    "request_id": "57233e1f-b153-4142-b278-29c755394394",
                },
                "hi!",
            ),
            AnyStringWith(f"recv channel msg, but channel not create. channel id:"),
        )

    async def test_read_not_parse_response(self, rap_server: Server, mocker: MockerFixture) -> None:
        await self._read_helper(
            mocker,
            (
                -1,
                -1,
                "faker_id",
                "/default/test_channel",
                200,
                {
                    "channel_life_cycle": "MSG",
                    "version": "0.1",
                    "user_agent": "Python3-0.5.3",
                    "request_id": "57233e1f-b153-4142-b278-29c755394394",
                },
                "hi!",
            ),
            AnyStringWith("Can' parse response:"),
        )

    async def test_request_receive_error_response_num(
        self, rap_server: Server, rap_client: Client, mocker: MockerFixture
    ) -> None:
        mock_future: asyncio.Future = asyncio.Future()
        mocker.patch("rap.client.transport.transport.Transport._base_request").return_value = mock_future
        mock_future.set_result(
            Response.from_msg(
                rap_client,
                rap_client.get_conn(),
                (
                    -1,
                    202,
                    f"{str(int(time.time()))}",
                    "/_event/default",
                    200,
                    {
                        "version": "0.1",
                        "user_agent": "Python3-0.5.3",
                        "request_id": "57233e1f-b153-4142-b278-29c755394394",
                    },
                    "hi!",
                ),
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
                rap_client,
                rap_client.get_conn(),
                (
                    29759,
                    201,
                    f"{str(int(time.time()))}",
                    "/_event/default",
                    200,
                    {
                        "version": "0.1",
                        "request_id": "fe41e811-3cd0-45e7-b83a-738759cb0ad8",
                        "host": ("127.0.0.1", 59022),
                    },
                    {"call_id": -1, "exc_info": "division by zero", "exc": "ZeroDivisionError"},
                ),
            )
        )

        with pytest.raises(RpcRunTimeError) as e:
            await rap_client.raw_call("sync_sum", [1, 2])

        exec_msg: str = e.value.args[0]
        assert exec_msg == "division by zero"
