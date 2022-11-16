import logging
from typing import TYPE_CHECKING, Optional, Tuple

from rap.common.asyncio_helper import Deadline
from rap.common.conn import ServerConnection
from rap.common.utils import constant
from rap.server.model import Event, Response, ServerContext
from rap.server.plugin.processor.base import BaseProcessor

if TYPE_CHECKING:
    from rap.server.core import Server
__all__ = ["Sender"]
logger: logging.Logger = logging.getLogger(__name__)


class Sender(object):
    """send data to conn"""

    def __init__(
        self,
        app: "Server",
        conn: ServerConnection,
        timeout: Optional[int] = None,
        processor: Optional[BaseProcessor] = None,
    ):
        """
        :param app: rap server
        :param conn: rap server conn
        :param timeout: send data timeout
        :param processor: rap server processor
        """
        self._app: "Server" = app
        self._max_correlation_id: int = 2 ** 16
        self._correlation_id: int = 2
        self._conn: ServerConnection = conn
        self._timeout: Optional[int] = timeout

        self._processor: Optional[BaseProcessor] = processor

    async def __call__(self, resp: Optional[Response], deadline: Optional[Deadline] = None) -> bool:
        """Send response data to the client"""
        if resp is None:
            return False

        response: Response = resp

        async def response_cb(is_raise: bool = True) -> Tuple[Response, Optional[Exception]]:
            return response, None

        if self._processor:
            response = await self._processor.process_response(response_cb)

        logger.debug("resp: %s", response)
        if not deadline:
            deadline = Deadline(self._timeout)

        with deadline:
            await self._conn.write(response.to_msg())
        if response.target.endswith(constant.CLOSE_EVENT):
            # conn will be closed after sending the close event
            if not self._conn.is_closed():
                await self._conn.await_close()
        return True

    #############################
    # Server-side push messages #
    #############################
    def _create_context(self) -> ServerContext:
        context: ServerContext = ServerContext()
        context.app = self._app
        context.conn = self._conn
        correlation_id: int = self._correlation_id + 2
        self._correlation_id = correlation_id % self._max_correlation_id
        context.correlation_id = correlation_id
        return context

    async def send_event(self, event: Event, deadline: Optional[Deadline] = None) -> bool:
        """send event obj to client"""
        response = Response.from_event(event, self._create_context())
        return await self.__call__(response, deadline=deadline)
