from typing import Any

import pytest
from pytest_mock import MockerFixture

from rap.client import Client, Response
from rap.common.channel import ChannelCloseError, UserChannel
from rap.common.exceptions import ChannelError, FuncNotFoundError
from rap.common.utils import constant
from rap.server import Response as ServerResponse
from rap.server import Server
from rap.server import UserChannel as ServerChannel

pytestmark = pytest.mark.asyncio


class TestChannel:
    async def test_while_channel(self, rap_server: Server, rap_client: Client) -> None:
        msg: str = "hello!"

        @rap_client.register_channel()
        async def async_channel(channel: UserChannel) -> None:
            await channel.write(msg)
            cnt: int = 0
            while await channel.loop(cnt < 3):
                cnt += 1
                await channel.write(msg)
                assert msg == await channel.read_body()
            return

        async def _async_channel(channel: ServerChannel) -> None:
            while await channel.loop():
                body: Any = await channel.read_body()
                await channel.write(body)

        rap_server.register(_async_channel, "async_channel")
        await async_channel()

    async def test_echo_body(self, rap_server: Server, rap_client: Client) -> None:
        @rap_client.register_channel()
        async def echo_body(channel: UserChannel) -> None:
            msg: str = "hello!"
            cnt: int = 0
            await channel.write(msg)
            async for body in channel.iter_body():
                assert body == msg
                if cnt >= 3:
                    break
                cnt += 1
                await channel.write(body)

        async def _echo_body(channel: UserChannel) -> None:
            async for body in channel.iter_body():
                await channel.write(body)

        rap_server.register(_echo_body, "echo_body")
        await echo_body()

    async def test_echo_response(self, rap_server: Server, rap_client: Client) -> None:
        @rap_client.register_channel()
        async def echo_response(channel: UserChannel) -> None:
            msg: str = "hello!"
            cnt: int = 0
            await channel.write(msg)
            async for response in channel.iter():
                # IDE cannot check
                response: Response = response  # type: ignore
                assert msg == response.body
                if cnt >= 3:
                    break
                cnt += 1
                await channel.write(response.body)

        async def _echo_response(channel: UserChannel) -> None:
            async for response in channel.iter():
                # IDE cannot check
                response: ServerResponse = response  # type: ignore
                await channel.write(response.body)

        rap_server.register(_echo_response, "echo_response")
        await echo_response()

    async def test_while_channel_close(self, rap_server: Server, rap_client: Client) -> None:
        @rap_client.register_channel()
        async def async_channel(channel: UserChannel) -> None:
            await channel.write("hello")
            cnt: int = 0
            while await channel.loop(cnt < 3):
                cnt += 1
                print(await channel.read_body())
            return

        async def _async_channel(channel: UserChannel) -> None:
            while await channel.loop():
                body: Any = await channel.read_body()
                if body == "hello":
                    break

                else:
                    await channel.write("I don't know")

        rap_server.register(_async_channel, "async_channel")
        with pytest.raises(ChannelCloseError) as e:
            await async_channel()

        exec_msg: str = e.value.args[0]
        assert exec_msg == "recv channel's drop event, close channel"

    async def test_not_found_channel_func(self, rap_server: Server, rap_client: Client) -> None:
        @rap_client.register_channel()
        async def async_channel(channel: UserChannel) -> None:
            await channel.write("hello")
            cnt: int = 0
            while await channel.loop(cnt < 3):
                cnt += 1
                print(await channel.read_body())
            return

        with pytest.raises(FuncNotFoundError) as e:
            await async_channel()

        exec_msg: str = e.value.args[0]
        assert exec_msg == "Not found func. name: async_channel"

    async def test_channel_not_create_when_recv_msg(
        self, rap_server: Server, rap_client: Client, mocker: MockerFixture
    ) -> None:
        mocker.patch("rap.client.transport.transport.Transport._gen_correlation_id").return_value = 234

        @rap_client.register_channel("test_channel")
        async def test_client_channel(channel: UserChannel) -> None:
            async for response in channel.iter():
                await channel.write("close")

        mocker.patch("rap.client.model.Request.to_msg").return_value = (
            constant.CHANNEL_REQUEST,
            234,
            {"channel_life_cycle": constant.MSG, "target": "/default/test_channel"},
            None,
        )
        with pytest.raises(ChannelError) as e:
            await test_client_channel()
        exec_msg = e.value.args[0]
        assert exec_msg == "channel not create"

    async def test_channel_not_create_when_recv_drop(
        self, rap_server: Server, rap_client: Client, mocker: MockerFixture
    ) -> None:
        mocker.patch("rap.client.transport.transport.Transport._gen_correlation_id").return_value = 234

        @rap_client.register_channel("test_channel")
        async def test_client_channel(channel: UserChannel) -> None:
            async for response in channel.iter():
                await channel.write("close")

        mocker.patch("rap.client.model.Request.to_msg").return_value = (
            constant.CHANNEL_REQUEST,
            234,
            {"channel_life_cycle": constant.DROP, "target": "/default/test_channel"},
            None,
        )
        with pytest.raises(ChannelError) as e:
            await test_client_channel()
        exec_msg = e.value.args[0]
        assert exec_msg == "channel not create"

    async def test_channel_life_cycle_error(
        self, rap_server: Server, rap_client: Client, mocker: MockerFixture
    ) -> None:
        mocker.patch("rap.client.transport.transport.Transport._gen_correlation_id").return_value = 234

        @rap_client.register_channel("test_channel")
        async def test_client_channel(channel: UserChannel) -> None:
            async for response in channel.iter():
                await channel.write("close")

        mocker.patch("rap.client.model.Request.to_msg").return_value = (
            constant.CHANNEL_REQUEST,
            234,
            {"channel_life_cycle": -1, "target": "/default/test_channel"},
            None,
        )
        with pytest.raises(ChannelError) as e:
            await test_client_channel()
        exec_msg = e.value.args[0]
        assert exec_msg == "channel life cycle error"
