import asyncio
import logging
import uuid
from typing import TYPE_CHECKING, Any, Callable, Coroutine, Optional, Tuple, Union

from rap.client.model import Request, Response
from rap.common.channel import BaseChannel
from rap.common.conn import Connection
from rap.common.exceptions import ChannelError
from rap.common.utlis import Constant

if TYPE_CHECKING:
    from rap.client.transoprt.transport import Session


__all__ = ["Channel"]


class Channel(BaseChannel):
    """support channel use"""

    def __init__(
        self,
        fun_name: str,
        session: "Session",
        create: Callable[[str], Coroutine[Any, Any, Any]],
        read: Callable[[str], Coroutine[Any, Any, Response]],
        write: Callable[[Request, "Session"], Coroutine[Any, Any, None]],
        close: Callable[[str], Coroutine[Any, Any, Any]],
        add_listen_conn_exc: Callable[[str, Connection], None],
    ):
        self.channel_id: str = str(uuid.uuid4())
        self._func_name: str = fun_name
        self._session: "Session" = session
        self._create: Callable[[str], Coroutine[Any, Any, Any]] = create
        self._read: Callable[[str], Coroutine[Any, Any, Response]] = read
        self._write: Callable[[Request, "Session"], Coroutine[Any, Any, None]] = write
        self._close: Callable[[str], Coroutine[Any, Any, Any]] = close
        self._add_listen_conn_exc: Callable[[str, Connection], None] = add_listen_conn_exc
        self._is_close: bool = True

    async def create(self):
        """create and init channel, create session and listen conn exc"""
        if not self._is_close:
            raise ChannelError("channel already create")

        # init channel data structure
        self._session.create()
        await self._create(self.channel_id)
        self._add_listen_conn_exc(self.channel_id, self._session.conn)
        self._is_close = False

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
        if self._is_close:
            raise ChannelError(f"channel is closed")
        response: Union[Response, Exception] = await self._read(self.channel_id)

        if isinstance(response, Exception):
            raise response
        if response.header.get("channel_life_cycle") == Constant.DROP:
            self._is_close = True
            await self._close(self.channel_id)
            raise ChannelError("recv drop event, close channel")
        return response

    async def _base_write(self, body: Any, life_cycle: str):
        """base send body to channel"""
        if self._is_close:
            raise ChannelError(f"channel is closed")
        request: Request = Request(
            Constant.CHANNEL_REQUEST,
            self._func_name,
            body,
            {"channel_life_cycle": life_cycle, "channel_id": self.channel_id},
        )
        await self._write(request, self._session)

    async def read(self) -> Response:
        response: Response = await self._base_read()
        if response.header.get("channel_life_cycle") != Constant.MSG:
            raise ChannelError("channel life cycle error")
        return response

    async def read_body(self):
        response: Response = await self.read()
        return response.body

    async def write(self, body: Any):
        await self._base_write(body, Constant.MSG)

    async def close(self):
        """Actively send a close message and close the channel"""
        if self._is_close:
            return
        life_cycle: str = Constant.DROP
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
        self._session.close()

    ######################
    # async with support #
    ######################
    async def __aenter__(self) -> "Channel":
        await self.create()
        return self

    async def __aexit__(self, *args: Tuple):
        await self.close()
