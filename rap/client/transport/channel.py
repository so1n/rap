import asyncio
import logging
import uuid
from typing import Any, Callable, Coroutine, Optional, Tuple, TYPE_CHECKING

from rap.client.model import Request, Response
from rap.common.channel import BaseChannel
from rap.common.conn import Connection
from rap.common.exceptions import ChannelError
from rap.common.utils import Constant, as_first_completed


if TYPE_CHECKING:
    from .transport import Transport
__all__ = ["Channel"]


class Channel(BaseChannel):
    """support channel use"""

    def __init__(
        self,
        transport: "Transport",
        target: str,
        conn: Connection,
        create: Callable[[str], Coroutine[Any, Any, Any]],
        read: Callable[[str], Coroutine[Any, Any, Response]],
        write: Callable[[Request], Coroutine[Any, Any, None]],
        close: Callable[[str], Coroutine[Any, Any, Any]],
    ):
        """
        target: rap target
        conn: channel transport conn
        create: rap's transport create channel queue func
        read: rap's transport read response func
        write: rap's transport write to conn func
        close: close queue func
        """
        self.channel_id: str = str(uuid.uuid4())
        self._transport: "Transport" = transport
        self._target: str = target
        self._conn: Connection = conn
        self._create: Callable[[str], Coroutine[Any, Any, Any]] = create
        self._read: Callable[[str], Coroutine[Any, Any, Response]] = read
        self._write: Callable[[Request], Coroutine[Any, Any, None]] = write
        self._close: Callable[[str], Coroutine[Any, Any, Any]] = close
        self._channel_conn_future: asyncio.Future = asyncio.Future()
        self._channel_conn_future.set_result(True)
        self._drop_msg: str = "recv drop event, close channel"

    async def create(self) -> None:
        """create and init channel, create session and listen conn exc"""
        if not self.is_close:
            raise ChannelError("channel already create")

        # init channel data structure
        await self._create(self.channel_id)
        self._channel_conn_future = asyncio.Future()

        def add_done_callback(f: asyncio.Future) -> None:
            if f.cancelled():
                self.set_exc(ChannelError("channel is close"))
            f_exc: Optional[BaseException] = f.exception()
            if f_exc:
                self.set_exc(f_exc)
            else:
                self.set_exc(ChannelError("channel is close"))

        self._conn.conn_future.add_done_callback(add_done_callback)

        # init with server
        life_cycle: str = Constant.DECLARE
        await self._base_write(None, life_cycle)
        response: Response = await self._base_read()
        if response.header.get("channel_life_cycle") != life_cycle:
            raise ChannelError("channel life cycle error")

    async def _base_read(self) -> Response:
        """base read response msg from channel conn
        When a drop message is received or the channel is closed, will raise `ChannelError`
        """
        if self.is_close:
            raise ChannelError(f"channel is closed")

        try:
            response: Response = await as_first_completed(
                [self._read(self.channel_id)],
                not_cancel_future_list=[self._channel_conn_future],
            )
        except Exception as e:
            raise e

        if response.header.get("channel_life_cycle") == Constant.DROP:
            await self._close(self.channel_id)
            exc: ChannelError = ChannelError(self._drop_msg)
            self.set_exc(exc)
            raise exc
        return response

    async def _base_write(self, body: Any, life_cycle: str) -> None:
        """base send body to channel"""
        if self.is_close:
            raise ChannelError(f"channel is closed")
        request: Request = Request(
            self._transport.app,
            Constant.CHANNEL_REQUEST,
            self._target,
            body,
            correlation_id=self.channel_id,
            header={"channel_life_cycle": life_cycle, "channel_id": self.channel_id},
        )
        await self._write(request)

    async def read(self) -> Response:
        response: Response = await self._base_read()
        if response.header.get("channel_life_cycle") != Constant.MSG:
            raise ChannelError("channel life cycle error")
        return response

    async def read_body(self) -> Any:
        response: Response = await self.read()
        return response.body

    async def write(self, body: Any) -> None:
        await self._base_write(body, Constant.MSG)

    async def close(self) -> None:
        """Actively send a close message and close the channel"""
        if self.is_close:
            try:
                await self._channel_conn_future
            except ChannelError:
                pass
            return

        await self._base_write(None, Constant.DROP)

        async def wait_drop_response() -> None:
            try:
                while True:
                    response: Response = await self._base_read()
                    logging.debug("drop msg:%s" % response)
            except ChannelError as e:
                if str(e) != self._drop_msg:
                    raise e

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

    async def __aexit__(self, *args: Tuple) -> None:
        await self.close()
