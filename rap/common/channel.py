from typing import Any


class BaseChannel(object):
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
        raise NotImplementedError

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
        raise NotImplementedError

    #####################
    # async for support #
    #####################
    def __aiter__(self) -> "BaseChannel":
        raise NotImplementedError

    async def __anext__(self):
        raise NotImplementedError
