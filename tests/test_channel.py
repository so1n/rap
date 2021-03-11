from typing import Any
import pytest

from rap.client import Client, Channel, Response
from rap.common.exceptions import FuncNotFoundError
from rap.server import Server, ResponseModel
from rap.server.requests import ChannelError


pytestmark = pytest.mark.asyncio


class TestChannel:

    async def test_while_channel(self, rap_server: Server, rap_client: Client) -> None:
        @rap_client.register()
        async def async_channel(channel: Channel) -> None:
            await channel.write("hello")
            cnt: int = 0
            while await channel.loop(cnt < 3):
                cnt += 1
                print(await channel.read_body())
            return

        async def _async_channel(channel: Channel) -> None:
            while await channel.loop():
                body: Any = await channel.read_body()
                if body == "hello":
                    cnt: int = 0
                    await channel.write(f"hello {cnt}")
                    while await channel.loop(cnt < 10):
                        cnt += 1
                        await channel.write(f"hello {cnt}")
                else:
                    await channel.write("I don't know")

        rap_server.register(_async_channel, "async_channel")
        await async_channel()

    async def test_echo_body(self, rap_server: Server, rap_client: Client) -> None:
        @rap_client.register()
        async def echo_body(channel: Channel) -> None:
            await channel.write("hi!")
            async for body in channel.iter_body():
                print(f"body:{body}")
                await channel.write(body)

        async def _echo_body(channel: Channel) -> None:
            cnt: int = 0
            async for body in channel.iter_body():
                cnt += 1
                if cnt > 10:
                    break
                await channel.write(body)
        rap_server.register(_echo_body, "echo_body")
        await echo_body()

    async def test_echo_response(self, rap_server: Server, rap_client: Client) -> None:
        @rap_client.register()
        async def echo_response(channel: Channel) -> None:
            await channel.write("hi!")
            async for response in channel.iter_response():
                response: Response = response  # IDE cannot check
                print(f"response: {response}")
                await channel.write(response.body)

        async def _echo_response(channel: Channel) -> None:
            cnt: int = 0
            async for response in channel.iter_response():
                response: ResponseModel = response  # IDE cannot check
                cnt += 1
                if cnt > 10:
                    break
                await channel.write(response.body)
        rap_server.register(_echo_response, "echo_response")
        await echo_response()

    async def test_while_channel_close(self, rap_server: Server, rap_client: Client) -> None:
        @rap_client.register()
        async def async_channel(channel: Channel) -> None:
            await channel.write("hello")
            cnt: int = 0
            while await channel.loop(cnt < 3):
                cnt += 1
                print(await channel.read_body())
            return

        async def _async_channel(channel: Channel) -> None:
            while await channel.loop():
                body: Any = await channel.read_body()
                if body == "hello":
                    cnt: int = 0
                    await channel.close()

                    with pytest.raises(ChannelError):
                        await channel.read_body()
                    with pytest.raises(ChannelError):
                        await channel.write(f"hello {cnt}")
                else:
                    await channel.write("I don't know")

        rap_server.register(_async_channel, "async_channel")
        with pytest.raises(ChannelError) as e:
            await async_channel()

        exec_msg: str = e.value.args[0]
        assert exec_msg == "recv drop event, close channel"

    async def test_not_found_channel_func(self, rap_server: Server, rap_client: Client) -> None:
        @rap_client.register()
        async def async_channel(channel: Channel) -> None:
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
