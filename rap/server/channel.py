import asyncio
import logging
from typing import TYPE_CHECKING, Any, Callable, Coroutine, Dict, Optional, Union

from rap.common.asyncio_helper import del_future
from rap.common.channel import BaseChannel, UserChannel, UserChannelType, get_corresponding_channel_class
from rap.common.conn import ServerConnection
from rap.common.exceptions import ChannelError
from rap.common.utils import constant

if TYPE_CHECKING:
    from rap.server import Request

logger: logging.Logger = logging.getLogger(__name__)


class Channel(BaseChannel["Request"]):
    """server channel support"""

    _user_channel: UserChannel

    def __init__(
        self,
        channel_id: int,
        write: Callable[[Any, Dict[str, Any]], Coroutine[Any, Any, Any]],
        conn: ServerConnection,
        func: Callable[["Channel"], Any],
    ):
        self._write: Callable[[Any, Dict[str, Any]], Coroutine[Any, Any, Any]] = write
        self._conn: ServerConnection = conn
        self._func: Callable = func
        self.queue: asyncio.Queue = asyncio.Queue()
        self.channel_id: int = channel_id

        # if conn close, channel future will done and channel not read & write_to_conn
        self.channel_conn_future: asyncio.Future = asyncio.Future()
        self.channel_conn_future.add_done_callback(lambda f: self.queue.put_nowait(f.exception()))

        self._conn.conn_future.add_done_callback(lambda f: self.set_exc(ChannelError("connection already close")))

        self.func_future: asyncio.Future = asyncio.ensure_future(self._run_func(func))

    @property
    def user_channel(self) -> UserChannelType:
        user_channel: Optional[UserChannelType] = getattr(self, "_user_channel", None)
        if not user_channel:
            user_channel = get_corresponding_channel_class(self._func)(self)
            setattr(self, "_user_channel", user_channel)
        return user_channel

    async def _run_func(self, func: Callable) -> None:
        try:
            await func(get_corresponding_channel_class(func)(self))
        except Exception as e:
            logger.debug("channel:%s, func: %s, ignore raise exc:%s", self.channel_id, func.__name__, e)
        finally:
            if not self.is_close:
                await self.close()

    async def write(self, body: Any) -> None:
        if self.is_close:
            raise ChannelError(f"channel<{self.channel_id}> is close")
        await self._write(body, {"channel_life_cycle": constant.MSG})

    async def read(self) -> "Request":
        if self.is_close:
            raise ChannelError(f"<channel{self.channel_id}> is close")
        result: Union["Request", Exception] = await self.queue.get()
        if isinstance(result, Exception):
            raise result
        return result

    async def read_body(self) -> Any:
        request: "Request" = await self.read()
        return request.body

    async def close(self) -> None:
        if self.is_close:
            logger.debug("already close channel %s", self.channel_id)
            return
        self.set_exc(ChannelError(f"channel {self.channel_id} is close"))

        if not self._conn.is_closed():
            await self._write(None, {"channel_life_cycle": constant.DROP})

        # Actively cancel the future may not be successful, such as cancel asyncio.sleep
        del_future(self.func_future)
