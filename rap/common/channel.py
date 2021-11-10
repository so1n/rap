import asyncio
from typing import Any, Generic, TypeVar

Read_T = TypeVar("Read_T")


class ChannelCloseError(Exception):
    """channel is close"""


class BaseChannel(Generic[Read_T]):
    """Common method of rap client and server channel"""

    channel_id: str
    channel_conn_future: asyncio.Future
    queue: asyncio.Queue

    async def read(self) -> Read_T:
        """read msg obj from channel"""
        raise NotImplementedError

    async def read_body(self) -> Any:
        """read body obj from channel's msg obj"""
        raise NotImplementedError

    async def write(self, body: Any) -> Any:
        """write_to_conn body to channel"""
        raise NotImplementedError

    async def close(self) -> Any:
        """close channel"""
        raise NotImplementedError

    @property
    def is_close(self) -> bool:
        """whether the channel is closed"""
        return self.channel_conn_future.done()

    async def wait_close(self) -> None:
        await self.channel_conn_future

    def set_exc(self, exc: BaseException) -> None:
        if self.channel_conn_future and not self.channel_conn_future.done():
            self.channel_conn_future.set_exception(exc)
            try:
                self.channel_conn_future.exception()
            except Exception:
                pass

    def set_success_finish(self) -> None:
        if self.channel_conn_future and not self.channel_conn_future.done():
            self.channel_conn_future.set_result(True)


class AsyncIterData(Generic[Read_T]):
    def __init__(self, channel: "BaseChannel"):
        self.channel = channel

    def __aiter__(self) -> "AsyncIterData":
        return self

    async def __anext__(self) -> "Read_T":
        try:
            return await self.channel.read()
        except ChannelCloseError:
            raise StopAsyncIteration()


class AsyncIterDataBody(AsyncIterData[Read_T]):
    async def __anext__(self) -> "Read_T":
        try:
            return await self.channel.read_body()
        except ChannelCloseError:
            raise StopAsyncIteration()


class UserChannel(Generic[Read_T]):
    """Only expose the user interface of BaseChannel"""

    def __init__(self, channel: "BaseChannel[Read_T]"):
        self._channel: BaseChannel[Read_T] = channel

    async def loop(self, flag: bool = True) -> bool:
        """In the channel function, elegantly replace `while True`

        bad demo
        >>> async def channel_demo(channel: UserChannel):
        ...     while True:
        ...         pass

        good demo
        >>> async def channel_demo(channel: UserChannel):
        ...     while await channel.loop():
        ...         pass

        bad demo
        >>> cnt: int = 0
        >>> async def channel_demo(channel: UserChannel):
        ...     while cnt < 3:
        ...         pass

        good demo
        >>> cnt: int = 0
        >>> async def channel_demo(channel: UserChannel):
        ...     while await channel.loop(cnt < 3):
        ...         pass
        """
        await asyncio.sleep(0)
        if self._channel.is_close:
            return False
        else:
            return flag

    async def read(self) -> Read_T:
        """read msg obj from channel"""
        return await self._channel.read()

    async def read_body(self) -> Any:
        """read body obj from channel's msg obj"""
        return await self._channel.read_body()

    async def write(self, body: Any) -> Any:
        """write_to_conn body to channel"""
        await self._channel.write(body)

    @property
    def channel_id(self) -> str:
        """channel id, each channel has a unique id"""
        return self._channel.channel_id

    @property
    def is_close(self) -> bool:
        """whether the channel is closed"""
        return self._channel.is_close

    async def wait_close(self) -> None:
        """wait channel close"""
        await self._channel.wait_close()

    #####################
    # async for support #
    #####################
    def iter(self) -> AsyncIterData[Read_T]:
        """
        >>> async def channel_demo(channel: UserChannel):
        ...     async for response in channel.iter():
        ...         response.body
        ...         response.header
        """
        return AsyncIterData(self._channel)

    def iter_body(self) -> AsyncIterDataBody[Read_T]:
        """
        >>> async def channel_demo(channel: UserChannel):
        ...     async for body in channel.iter():
        ...         print(body)
        """
        return AsyncIterDataBody(self._channel)
