import asyncio
import inspect
from typing import Any, Awaitable, Callable, Generic, List, Optional, Type, TypeVar, Union, overload

from typing_extensions import Self

from rap.common.types import get_real_annotation

_Read_T = TypeVar("_Read_T")


class ChannelCloseError(Exception):
    """channel is close"""


class BaseChannel(Generic[_Read_T]):
    """Common method of rap client and server channel"""

    channel_id: int
    channel_future: asyncio.Future
    queue: asyncio.Queue

    async def read(self, timeout: Optional[int] = None) -> _Read_T:
        """read msg obj from channel"""
        raise NotImplementedError

    async def read_body(self, timeout: Optional[int] = None) -> Any:
        """read body obj from channel's msg obj"""
        raise NotImplementedError

    async def write(self, body: Any, header: Optional[dict] = None, timeout: Optional[int] = None) -> Any:
        """write_to_conn body to channel"""
        raise NotImplementedError

    async def close(self) -> Any:
        """close channel"""
        raise NotImplementedError

    @property
    def is_close(self) -> bool:
        """whether the channel is closed"""
        return self.channel_future.done()

    async def wait_close(self) -> None:
        await self.channel_future

    def set_finish(self, exc: Optional[Exception] = None) -> None:
        """Set the channel to end running, if exc is not empty, exc will be set"""
        if not (self.channel_future and not self.channel_future.done()):
            return
        if exc is None:
            self.channel_future.set_result(True)
        else:
            self.channel_future.set_exception(exc)
            try:
                self.channel_future.exception()
            except Exception:
                pass

    ###############
    # Get Channel #
    ###############
    @overload
    def get_user_channel_from_func(self, func: None) -> "UserChannel":
        ...

    @overload
    def get_user_channel_from_func(self, func: Callable[["ReadChannel"], Awaitable[None]]) -> "ReadChannel":
        ...

    @overload
    def get_user_channel_from_func(self, func: Callable[["WriteChannel"], Awaitable[None]]) -> "WriteChannel":
        ...

    @overload
    def get_user_channel_from_func(self, func: Callable[["UserChannel"], Awaitable[None]]) -> "UserChannel":
        ...

    def get_user_channel_from_func(self, func):
        if func is None:
            user_channel = UserChannel(self)
        else:
            user_channel = get_corresponding_channel_class(func)(self)
        return user_channel

    def get_read_channel(self) -> "ReadChannel":
        return ReadChannel(self)

    def get_write_channel(self) -> "WriteChannel":
        return WriteChannel(self)

    def get_user_channel(self) -> "UserChannel":
        return UserChannel(self)

    def get_context_channel(self) -> "ContextChannel":
        return ContextChannel(self)


class _AsyncIterData(Generic[_Read_T]):
    def __init__(self, channel: "BaseChannel", timeout: Optional[int] = None):
        self.channel = channel
        self.timeout: Optional[int] = timeout

    def __aiter__(self) -> "Self":
        return self

    async def __anext__(self) -> "_Read_T":
        try:
            return await self.channel.read(timeout=self.timeout)
        except ChannelCloseError:
            raise StopAsyncIteration()


class _AsyncIterDataBody(_AsyncIterData[_Read_T]):
    async def __anext__(self) -> "_Read_T":
        try:
            return await self.channel.read_body(timeout=self.timeout)
        except ChannelCloseError:
            raise StopAsyncIteration()


class BaseUserChannel(Generic[_Read_T]):
    """Provides a generic way that users can use channels (no IO involved)"""

    def __init__(self, channel: "BaseChannel[_Read_T]"):
        self._channel: BaseChannel[_Read_T] = channel

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
        return False if self._channel.is_close else flag

    @property
    def channel_id(self) -> int:
        """channel id, each channel has a unique id"""
        return self._channel.channel_id

    @property
    def is_close(self) -> bool:
        """whether the channel is closed"""
        return self._channel.is_close

    async def wait_close(self) -> None:
        """wait channel close"""
        await self._channel.wait_close()

    def add_done_callback(self, fn: Callable[[asyncio.Future], None]) -> None:
        self._channel.channel_future.add_done_callback(fn)

    def remove_done_callback(self, fn: Callable[[asyncio.Future], None]) -> None:
        self._channel.channel_future.remove_done_callback(fn)


class _ReadChannelMixin(Generic[_Read_T]):
    """Provides methods for reading data from a channel"""

    _channel: "BaseChannel[_Read_T]"

    async def read(self, timeout: Optional[int] = None) -> _Read_T:
        """read msg obj from channel"""
        return await self._channel.read(timeout=timeout)

    async def read_body(self, timeout: Optional[int] = None) -> Any:
        """read body obj from channel's msg obj"""
        return await self._channel.read_body(timeout=timeout)

    #####################
    # async for support #
    #####################
    def iter(self, timeout: Optional[int] = None) -> _AsyncIterData[_Read_T]:
        """
        >>> async def channel_demo(channel: UserChannel):
        ...     async for response in channel.iter():
        ...         response.body
        ...         response.header
        """
        return _AsyncIterData(self._channel, timeout=timeout)

    def iter_body(self, timeout: Optional[int] = None) -> _AsyncIterDataBody[_Read_T]:
        """
        >>> async def channel_demo(channel: UserChannel):
        ...     async for body in channel.iter_body():
        ...         print(body)
        """
        return _AsyncIterDataBody(self._channel, timeout=timeout)

    def __aiter__(self) -> "Self":
        return self

    async def __anext__(self) -> Any:
        try:
            return await self._channel.read_body()
        except ChannelCloseError:
            raise StopAsyncIteration()


class _WriteChannelMixin(Generic[_Read_T]):
    """Provides methods for write data from a channel"""

    _channel: "BaseChannel[_Read_T]"

    async def write(self, body: Any, header: Optional[dict] = None, timeout: Optional[int] = None) -> Any:
        """write_to_conn body to channel"""
        await self._channel.write(body, header=header, timeout=timeout)


class ContextChannel(BaseUserChannel[_Read_T]):
    """Used for the channel obtained through the context, the channel will not provide functions related to IO"""


class ReadChannel(BaseUserChannel[_Read_T], _ReadChannelMixin):
    """Provides a generic way that users can use channels (with read)"""


class WriteChannel(BaseUserChannel[_Read_T], _WriteChannelMixin):
    """Provides a generic way that users can use channels (with write)"""


class UserChannel(BaseUserChannel[_Read_T], _WriteChannelMixin, _ReadChannelMixin):
    """Only expose the user interface of BaseChannel"""


UserChannelType = Union[ReadChannel, WriteChannel, UserChannel]
UserChannelCovariantType = TypeVar("UserChannelCovariantType", bound=BaseUserChannel, covariant=True)
UserChannelContravariantType = TypeVar("UserChannelContravariantType", bound=UserChannel, contravariant=True)


def get_opposite_channel_class(
    channel_class: Type[Union[WriteChannel, ReadChannel]]
) -> Type[Union[WriteChannel, ReadChannel]]:
    if channel_class == ReadChannel:
        return WriteChannel
    elif channel_class == WriteChannel:
        return ReadChannel
    else:
        raise TypeError(f"{channel_class} is not a valid channel class")


def get_corresponding_channel_class(func: Callable) -> Type[UserChannelType]:
    """Identify the corresponding channel through the function signature of func"""
    annotation: Any = getattr(func, "__channel_class__", None)
    if annotation:
        return annotation

    func_arg_parameter: List[inspect.Parameter] = [
        i for i in inspect.signature(func).parameters.values() if i.default == i.empty
    ]
    if len(func_arg_parameter) != 1:
        raise TypeError(f"func:{func.__name__} must channel function")
    annotation = get_real_annotation(func_arg_parameter[0].annotation, func)
    origin_type = getattr(annotation, "__origin__", None)
    if origin_type:
        annotation = origin_type
    if annotation not in (ReadChannel, WriteChannel, UserChannel):
        raise TypeError(f"func:{func.__name__} must channel function")
    setattr(func, "__channel_class__", annotation)
    return annotation
