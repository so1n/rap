import asyncio
from typing import TYPE_CHECKING, Any, Union

from rap.common.exceptions import ChannelError

if TYPE_CHECKING:
    from rap.client.model import Response
    from rap.server.model import ResponseModel


class AsyncIterResponse(object):
    def __init__(self, channel: "BaseChannel"):
        self.channel = channel

    def __aiter__(self) -> "AsyncIterResponse":
        return self

    async def __anext__(self) -> "Union[Response, ResponseModel]":
        try:
            return await self.channel.read()
        except ChannelError:
            raise StopAsyncIteration()


class AsyncIterBody(AsyncIterResponse):
    async def __anext__(self) -> "Union[Response, ResponseModel]":
        try:
            return await self.channel.read_body()
        except ChannelError:
            raise StopAsyncIteration()


class BaseChannel(object):
    _is_close: bool

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
        if self._is_close:
            return not self._is_close
        else:
            return flag

    async def read(self) -> Any:
        """read msg obj from channel"""
        raise NotImplementedError

    async def read_body(self):
        """read body obj from channel's msg obj"""
        raise NotImplementedError

    async def write(self, body: Any):
        """write body to channel"""
        raise NotImplementedError

    async def close(self):
        """close channel"""
        raise NotImplementedError

    @property
    def is_close(self) -> bool:
        """whether the channel is closed"""
        return self._is_close

    #####################
    # async for support #
    #####################
    def iter_response(self) -> AsyncIterResponse:
        return AsyncIterResponse(self)

    def iter_body(self) -> AsyncIterBody:
        return AsyncIterBody(self)
