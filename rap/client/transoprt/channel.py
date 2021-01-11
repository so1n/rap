import asyncio
import logging
import uuid
from typing import TYPE_CHECKING, Any, Callable, Coroutine, Tuple, Union

from rap.client.model import Request, Response
from rap.common.channel import BaseChannel
from rap.common.exceptions import ChannelError
from rap.common.utlis import Constant

if TYPE_CHECKING:
    from rap.client.transoprt.transport import Session


class Channel(BaseChannel):
    """support channel use"""

    def __init__(
        self,
        fun_name: str,
        session: "Session",
        create: Callable[[str], Coroutine[Any, Any, Any]],
        read: Callable[[str], Coroutine[Any, Any, Response]],
        write: Callable[[Request], Coroutine[Any, Any, str]],
        close: Callable[[str], Coroutine[Any, Any, Any]],
    ):
        self.channel_id: str = str(uuid.uuid4())
        self._func_name: str = fun_name
        self._session: "Session" = session
        self._create: Callable[[str], Coroutine[Any, Any, Any]] = create
        self._read: Callable[[str], Coroutine[Any, Any, Response]] = read
        self._write: Callable[[Request], Coroutine[Any, Any, str]] = write
        self._close: Callable[[str], Coroutine[Any, Any, Any]] = close
        self._is_close: bool = True

    async def create(self):
        """create and init channel, create session"""
        if not self._is_close:
            raise ChannelError("channel already create")
        self._session.create()
        await self._create(self.channel_id)
        self._is_close = False

        life_cycle: str = "declare"
        await self._base_write(None, life_cycle)
        response: Response = await self._base_read()
        if response.header.get("channel_life_cycle") != life_cycle:
            raise ChannelError("channel life cycle error")
        channel_id: str = response.header.get("channel_id")
        self.channel_id = channel_id

    async def _base_read(self) -> Response:
        """base read response msg from channel conn
        When a drop message is received or the channel is closed, will raise `ChannelError`
        """
        if self._is_close:
            raise ChannelError(f"channel is closed")
        response: Union[Response, Exception] = await self._read(self.channel_id)
        if isinstance(response, Exception):
            raise response
        if response.header.get("channel_life_cycle") == "drop":
            self._is_close = True
            await self._close(self.channel_id)
            raise ChannelError("recv drop event, close channel")
        return response

    async def loop(self, flag: bool = True) -> bool:
        """In the channel function, elegantly replace `while True`
        bad demo
        >>> async def channel_demo(channel: Channel):
        ...     while True:
        ...         pass

        good demo
        >>> async def channel_demo(channel: Channel):
        ...     while await channel.loop():
        ...         pass

        bad demo
        >>> cnt: int = 0
        >>> async def channel_demo(channel: Channel):
        ...     while cnt < 3:
        ...         pass

        good demo
        >>> cnt: int = 0
        >>> async def channel_demo(channel: Channel):
        ...     while await channel.loop(cnt < 3):
        ...         pass
        """
        await asyncio.sleep(0.01)
        if self._is_close:
            return not self._is_close
        else:
            return flag

    async def _base_write(self, body: Any, life_cycle: str) -> str:
        """base send body to channel"""
        if self._is_close:
            raise ChannelError(f"channel is closed")
        request: Request = Request(
            Constant.MSG_REQUEST,
            self._func_name,
            Constant.CHANNEL,
            body,
            {"channel_life_cycle": life_cycle, "channel_id": self.channel_id},
        )
        return await self._write(request)

    async def read(self) -> Response:
        response: Response = await self._base_read()
        if response.header.get("channel_life_cycle") != "msg":
            raise ChannelError("channel life cycle error")
        return response

    async def read_body(self):
        response: Response = await self.read()
        return response.body

    async def write(self, body: Any):
        await self._base_write(body, "msg")

    async def close(self):
        """Actively send a close message and close the channel"""
        if self._is_close:
            return
        self._session.close()
        life_cycle: str = "drop"
        await self._base_write(None, life_cycle)

        async def wait_drop_response():
            try:
                while True:
                    response: Response = await self._base_read()
                    logging.debug("drop msg:%s" % response)
            except ChannelError:
                pass

        try:
            await asyncio.wait_for(wait_drop_response(), 3)
        except asyncio.TimeoutError:
            logging.warning("wait drop response timeout")

    ######################
    # async with support #
    ######################
    async def __aenter__(self) -> "Channel":
        await self.create()
        return self

    async def __aexit__(self, *args: Tuple):
        await self.close()

    #####################
    # async for support #
    #####################
    def __aiter__(self) -> "Channel":
        return self

    async def __anext__(self):
        try:
            return await self.read_body()
        except ChannelError:
            raise StopAsyncIteration()

    @property
    def is_close(self) -> bool:
        return self._is_close
