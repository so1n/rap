import asyncio
import logging
import traceback
from typing import TYPE_CHECKING, Any, Optional, Type, Union

from typing_extensions import Self

from rap.client.model import ClientContext, Request, Response
from rap.common.asyncio_helper import as_first_completed, get_deadline
from rap.common.channel import BaseChannel, ChannelCloseError
from rap.common.channel import UserChannel as _UserChannel
from rap.common.exceptions import ChannelError, ChannelLifecycleError
from rap.common.utils import constant, ignore_exception

if TYPE_CHECKING:
    from .transport import Transport
__all__ = ["Channel", "UserChannel"]
UserChannel = _UserChannel[Response]


logger: logging.Logger = logging.getLogger(__name__)


class Channel(BaseChannel[Response]):
    """client channel support"""

    _user_channel: UserChannel

    def __init__(
        self,
        *,
        transport: "Transport",
        target: str,
        channel_id: int,
        context: ClientContext,
    ):
        """
        :param transport: rap client transport
        :param target: rap target
        :param channel_id: transport correlation_id
        """
        self._transport: "Transport" = transport
        self._target: str = target
        self._drop_msg: str = "recv channel's drop event, close channel"
        self.context: ClientContext = context

        self.channel_id: int = channel_id
        self.queue: asyncio.Queue[Union[Response, Exception]] = asyncio.Queue()
        self.context.context_channel = self.get_context_channel()
        self.channel_conn_future: asyncio.Future = asyncio.Future()

    async def create(self) -> None:
        """create and init channel, create session and listen transport exc"""

        def add_done_callback(f: asyncio.Future) -> None:
            """When the transport terminates, propagate the transport exception to the channel"""
            if f.cancelled():
                self.set_finish(ChannelCloseError("transport is cancel"))
            try:
                f.exception()
            except Exception as e:
                self.set_finish(e)
            else:
                self.set_finish(ChannelCloseError("channel's is close"))

        self._transport.add_close_callback(add_done_callback)

        # init with server
        life_cycle: str = constant.DECLARE
        await self._base_write(None, life_cycle, target=self._target)
        response: Response = await self._base_read()
        if response.header.get("channel_life_cycle") != life_cycle:
            raise ChannelLifecycleError()

    async def _base_read(self, timeout: Optional[int] = None) -> Response:
        """base read response msg from channel transport

        When a drop message is received , will raise `ChannelCloseError` and terminate the channel
        :param timeout: read msg from channel transport timeout
        """

        if self.is_close:
            raise ChannelCloseError("channel is closed")

        try:
            response: Union[Response, Exception] = await as_first_completed(
                [get_deadline(timeout).wait_for(self.queue.get())],
                not_cancel_future_list=[self.channel_conn_future],
            )
        except asyncio.TimeoutError:
            raise ChannelError(f"channel<{self.channel_id}> read response timeout")
        except Exception as e:
            raise e

        if isinstance(response, Exception):
            raise response
        if response.exc:
            raise response.exc

        if response.header.get("channel_life_cycle") == constant.DROP:
            exc: ChannelCloseError = ChannelCloseError(self._drop_msg)
            self.set_finish(exc)
            raise exc
        return response

    async def _base_write(
        self,
        body: Any,
        life_cycle: str,
        timeout: Optional[int] = None,
        target: Optional[str] = None,
        header: Optional[dict] = None,
    ) -> None:
        """base send body to channel"""
        if self.is_close:
            raise ChannelCloseError("channel is closed")
        header = header or {}
        header["channel_life_cycle"] = life_cycle
        request: Request = Request(
            msg_type=constant.CHANNEL_REQUEST,
            target=target,
            body=body,
            header=header,
            context=self.context,
        )
        await get_deadline(timeout).wait_for(self._transport.write_to_conn(request))

    async def read(self, timeout: Optional[int] = None) -> Response:
        response: Response = await self._base_read(timeout=timeout)
        if response.header.get("channel_life_cycle") != constant.MSG:
            raise ChannelLifecycleError("channel life cycle error")
        return response

    async def read_body(self, timeout: Optional[int] = None) -> Any:
        response: Response = await self.read(timeout=timeout)
        return response.body

    async def write(self, body: Any, header: Optional[dict] = None, timeout: Optional[int] = None) -> None:
        """
        :param body: send body
        :param timeout: wait write timeout
            In general, the write method is very fast,
            but in extreme cases transport has accumulated some requests and needs to wait
        """
        await self._base_write(body, constant.MSG, header=header, timeout=timeout)

    async def close(self) -> None:
        """Actively send a close message and close the channel"""
        if self.is_close:
            return

        await self._base_write(None, constant.DROP)

        with ignore_exception(ChannelCloseError):
            try:
                await asyncio.wait_for(self.wait_close(), 3)
            except asyncio.TimeoutError:
                logger.warning("wait drop response timeout")

    ######################
    # async with support #
    ######################
    async def __aenter__(self) -> "Self":
        await self.create()
        return self

    async def __aexit__(self, exc_type: Type[Exception], exc: str, tb: traceback.TracebackException) -> None:
        if exc_type:
            self.set_finish(exc_type(exc))
        else:
            self.set_finish()
        await self.close()
