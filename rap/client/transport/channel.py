import asyncio
import logging
import traceback
from typing import TYPE_CHECKING, Any, Coroutine, Optional, Tuple, Type

from rap.client.model import Request, Response
from rap.common.asyncio_helper import as_first_completed
from rap.common.channel import BaseChannel, ChannelCloseError, UserChannel
from rap.common.exceptions import ChannelError
from rap.common.state import State
from rap.common.utils import constant

if TYPE_CHECKING:
    from .transport import Transport
__all__ = ["Channel"]


logger: logging.Logger = logging.getLogger(__name__)


class Channel(BaseChannel[Response]):
    """client channel support"""

    def __init__(self, transport: "Transport", target: str, channel_id: int):
        """
        :param transport: rap client transport
        :param target: rap target
        :param channel_id: transport correlation_id
        """
        self._transport: "Transport" = transport
        self._target: str = target
        self._drop_msg: str = "recv channel's drop event, close channel"
        self.state: State = State()

        self.channel_id: int = channel_id
        self.queue: asyncio.Queue[Tuple[Response, Optional[Exception]]] = asyncio.Queue()
        self.user_channel: UserChannel[Response] = UserChannel(self)
        self.state.user_channel = self.user_channel
        self.channel_conn_future: asyncio.Future = asyncio.Future()

    async def create(self) -> None:
        """create and init channel, create session and listen transport exc"""

        def add_done_callback(f: asyncio.Future) -> None:
            if f.cancelled():
                self.set_exc(ChannelCloseError("channel's transport is close"))
            try:
                f.exception()
            except Exception as e:
                self.set_exc(e)
            else:
                self.set_exc(ChannelCloseError("channel's transport is close"))

        self._transport._conn.conn_future.add_done_callback(add_done_callback)

        # init with server
        life_cycle: str = constant.DECLARE
        await self._base_write(None, life_cycle)
        response: Response = await self._base_read()
        if response.header.get("channel_life_cycle") != life_cycle:
            raise ChannelError("channel life cycle error")

    async def _base_read(self, timeout: Optional[int] = None) -> Response:
        """base read response msg from channel transport
        When a drop message is received , will raise `ChannelError`
        :param timeout: read msg from channel transport timeout
        """

        if self.is_close:
            raise ChannelCloseError("channel is closed")

        async def _read_by_queue() -> Response:
            """read response or exc from queue"""
            _response, _exc = await self.queue.get()
            _response = await self._transport.process_response(_response, _exc)
            return _response

        try:
            response: Response = await as_first_completed(
                [asyncio.wait_for(_read_by_queue(), timeout=timeout)],
                not_cancel_future_list=[self.channel_conn_future],
            )
        except asyncio.TimeoutError:
            raise ChannelError(f"channel<{self.channel_id}> read response timeout")
        except Exception as e:
            raise e

        if response.header.get("channel_life_cycle") == constant.DROP:
            exc: ChannelCloseError = ChannelCloseError(self._drop_msg)
            self.set_exc(exc)
            raise exc
        return response

    async def _base_write(self, body: Any, life_cycle: str, timeout: Optional[int] = None) -> None:
        """base send body to channel"""
        if self.is_close:
            raise ChannelCloseError("channel is closed")
        request: Request = Request(
            app=self._transport.app,
            msg_type=constant.CHANNEL_REQUEST,
            target=self._target,
            body=body,
            correlation_id=self.channel_id,
            header={"channel_life_cycle": life_cycle},
            state=self.state,
        )
        coro: Coroutine = self._transport.write_to_conn(request)
        await asyncio.wait_for(coro, timeout)

    async def read(self, timeout: Optional[int] = None) -> Response:
        response: Response = await self._base_read(timeout=timeout)
        if response.header.get("channel_life_cycle") != constant.MSG:
            raise ChannelError("channel life cycle error")
        return response

    async def read_body(self, timeout: Optional[int] = None) -> Any:
        response: Response = await self.read(timeout=timeout)
        return response.body

    async def write(self, body: Any, timeout: Optional[int] = None) -> None:
        """
        :param body: send body
        :param timeout: wait write timeout
            In general, the write method is very fast,
            but in extreme cases transport has accumulated some requests and needs to wait
        """
        await self._base_write(body, constant.MSG, timeout=timeout)

    async def close(self) -> None:
        """Actively send a close message and close the channel"""
        if self.is_close:
            try:
                await self.channel_conn_future
            except ChannelCloseError:
                pass
            return

        await self._base_write(None, constant.DROP)

        async def wait_drop_response() -> None:
            try:
                while True:
                    response: Response = await self._base_read()
                    logger.debug("drop msg:%s" % response)
            except ChannelError as e:
                if str(e) != self._drop_msg:
                    raise e

        try:
            await asyncio.wait_for(wait_drop_response(), 3)
        except asyncio.TimeoutError:
            logger.warning("wait drop response timeout")

    ######################
    # async with support #
    ######################
    async def __aenter__(self) -> UserChannel[Response]:
        await self.create()
        return self.user_channel

    async def __aexit__(self, exc_type: Type[Exception], exc: str, tb: traceback.TracebackException) -> None:
        if exc_type:
            self.set_exc(exc_type(exc))
        else:
            self.set_success_finish()
        await self.close()
