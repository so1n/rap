import asyncio
from typing import TYPE_CHECKING, Any, Union

from rap.common.exceptions import ChannelError

if TYPE_CHECKING:
    from rap.client.model import Response
    from rap.server.model import Response as ServerResponse


class AsyncIterResponse(object):
    def __init__(self, channel: "BaseChannel"):
        self.channel = channel

    def __aiter__(self) -> "AsyncIterResponse":
        return self

    async def __anext__(self) -> "Union[Response, ServerResponse]":
        try:
            return await self.channel.read()
        except ChannelError:
            raise StopAsyncIteration()


class AsyncIterBody(AsyncIterResponse):
    async def __anext__(self) -> "Union[Response, ServerResponse]":
        try:
            return await self.channel.read_body()
        except ChannelError:
            raise StopAsyncIteration()


class BaseChannel(object):
    _channel_conn_future: asyncio.Future

    async def loop(self, flag: bool = True) -> bool:
        """In the channel function, elegantly replace `while True`
        bad demo
        >>> async def channel_demo(channel: BaseChannel):
        ...     while True:
        ...         pass

        good demo
        >>> async def channel_demo(channel: BaseChannel):
        ...     while await channel.loop():
        ...         pass

        bad demo
        >>> cnt: int = 0
        >>> async def channel_demo(channel: BaseChannel):
        ...     while cnt < 3:
        ...         pass

        good demo
        >>> cnt: int = 0
        >>> async def channel_demo(channel: BaseChannel):
        ...     while await channel.loop(cnt < 3):
        ...         pass
        """
        await asyncio.sleep(0)
        if self.is_close:
            return False
        else:
            return flag

    async def read(self) -> Any:
        """read msg obj from channel"""
        raise NotImplementedError

    async def read_body(self) -> Any:
        """read body obj from channel's msg obj"""
        raise NotImplementedError

    async def write(self, body: Any) -> Any:
        """write body to channel"""
        raise NotImplementedError

    async def close(self) -> Any:
        """close channel"""
        raise NotImplementedError

    @property
    def is_close(self) -> bool:
        """whether the channel is closed"""
        return self._channel_conn_future.done()

    async def wait_close(self) -> None:
        await self._channel_conn_future

    def set_finish(self, msg: str = "") -> None:
        if self._channel_conn_future and not self._channel_conn_future.done():
            self._channel_conn_future.set_exception(ChannelError(msg))
            # NOTE: ignore `Future exception was never retrieved` tip
            try:
                self._channel_conn_future.exception()
            except ChannelError:
                pass

    #####################
    # async for support #
    #####################
    def iter_response(self) -> AsyncIterResponse:
        return AsyncIterResponse(self)

    def iter_body(self) -> AsyncIterBody:
        return AsyncIterBody(self)
