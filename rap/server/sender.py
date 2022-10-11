import logging
from typing import TYPE_CHECKING, Callable, List, Optional

from rap.common.asyncio_helper import Deadline
from rap.common.conn import ServerConnection
from rap.common.exceptions import IgnoreNextProcessor
from rap.common.utils import constant
from rap.server.model import Event, Response, ServerContext
from rap.server.plugin.processor.base import BaseProcessor, belong_to_base_method

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
        processor_list: Optional[List[BaseProcessor]] = None,
    ):
        """
        :param app: rap server
        :param conn: rap server conn
        :param timeout: send data timeout
        :param processor_list: rap server processor list
        """
        self._app: "Server" = app
        self._max_correlation_id: int = 2 ** 16
        self._correlation_id: int = 2
        self._conn: ServerConnection = conn
        self._timeout: Optional[int] = timeout

        self._processor_list: Optional[List[BaseProcessor]] = processor_list
        processor_list = processor_list or []
        self.process_response_processor_list: List[Callable] = [
            i.process_response for i in processor_list if not belong_to_base_method(i.process_response)
        ]
        self.process_exception_processor_list: List[Callable] = [
            i.process_exc for i in processor_list if not belong_to_base_method(i.process_exc)
        ]

    async def _processor_response_handle(self, resp: Response) -> Response:
        if not self._processor_list:
            return resp
        if not resp.exc:
            # Once there is an exception, the processing logic of the response is exited directly
            try:
                for process_response in reversed(self.process_response_processor_list):
                    resp = await process_response(resp)
            except IgnoreNextProcessor:
                pass
            except Exception as e:
                resp.set_exception(e)
        if resp.exc:
            # Ensure that each Processor can handle exceptions
            for process_exc in reversed(self.process_exception_processor_list):
                raw_resp: Response = resp
                try:
                    resp = await process_exc(resp)
                except IgnoreNextProcessor:
                    break
                except Exception as e:
                    logger.exception(f"processor:{process_exc} handle response:{resp.correlation_id} error:{e}")
                    resp = raw_resp
        return resp

    async def __call__(self, resp: Optional[Response], deadline: Optional[Deadline] = None) -> bool:
        """Send response data to the client"""
        if resp is None:
            return False

        resp = await self._processor_response_handle(resp)
        logger.debug("resp: %s", resp)
        if not deadline:
            deadline = Deadline(self._timeout)

        with deadline:
            await self._conn.write(resp.to_msg())
        if resp.target.endswith(constant.CLOSE_EVENT):
            # conn will be closed after sending the close event
            if not self._conn.is_closed():
                await self._conn.await_close()
        return True

    async def response_event(self, event: Event, context: ServerContext, deadline: Optional[Deadline] = None) -> bool:
        return await self.__call__(Response.from_event(event, context), deadline=deadline)

    async def response_exc(self, exc: Exception, context: ServerContext, deadline: Optional[Deadline] = None) -> bool:
        return await self.__call__(Response.from_exc(exc, context), deadline=deadline)

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

    async def send_exc(self, exc: Exception, deadline: Optional[Deadline] = None) -> bool:
        """send exc obj to client"""
        response = Response.from_exc(exc, self._create_context())
        return await self.__call__(response, deadline=deadline)
