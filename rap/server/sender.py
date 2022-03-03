import logging
from typing import TYPE_CHECKING, Any, List, Optional
from uuid import uuid4

from rap.common.asyncio_helper import Deadline
from rap.common.conn import ServerConnection
from rap.common.exceptions import IgnoreNextProcessor
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
        processor_list: Optional[List[BaseProcessor]] = None,
    ):
        """
        :param app: rap server
        :param conn: rap server conn
        :param timeout: send data timeout
        :param
        """
        self._app: "Server" = app
        self._max_correlation_id: int = 65535
        self._correlation_id: int = 2
        self._conn: ServerConnection = conn
        self._timeout: Optional[int] = timeout
        self._processor_list: Optional[List[BaseProcessor]] = processor_list

    @staticmethod
    def header_handle(resp: Response) -> None:
        """response header handle"""

        def set_header_value(header_key: str, header_value: Any, is_cover: bool = False) -> None:
            """set header value"""
            if is_cover:
                resp.header[header_key] = header_value
            elif header_key not in resp.header:
                resp.header[header_key] = header_value

        set_header_value("version", constant.VERSION, is_cover=True)
        set_header_value("user_agent", constant.USER_AGENT, is_cover=True)
        set_header_value("request_id", str(uuid4()), is_cover=resp.msg_type is constant.CHANNEL_RESPONSE)

    async def _processor_response_handle(self, resp: Response) -> Response:
        if not self._processor_list:
            return resp
        if not resp.exc:
            try:
                for processor in reversed(self._processor_list):
                    resp = await processor.process_response(resp)
            except IgnoreNextProcessor:
                pass
            except Exception as e:
                resp.set_exception(e)
        if resp.exc:
            for processor in reversed(self._processor_list):
                raw_resp: Response = resp
                try:
                    resp, resp.exc = await processor.process_exc(resp, resp.exc)  # type: ignore
                except IgnoreNextProcessor:
                    break
                except Exception as e:
                    logger.exception(
                        f"processor:{processor.__class__.__name__} handle response:{resp.correlation_id} error:{e}"
                    )
                    resp = raw_resp
        return resp

    async def __call__(self, resp: Optional[Response], deadline: Optional[Deadline] = None) -> bool:
        """Send response data to the client"""
        if resp is None:
            return False

        self.header_handle(resp)
        resp = await self._processor_response_handle(resp)
        logger.debug("resp: %s", resp)
        if not deadline:
            deadline = Deadline(self._timeout)

        with deadline:
            await self._conn.write(resp.to_msg())
        if resp.target.endswith(constant.EVENT_CLOSE_CONN):
            if not self._conn.is_closed():
                self._conn.close()
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
        self._correlation_id = correlation_id & self._max_correlation_id
        context.correlation_id = correlation_id
        return context

    async def send_event(self, event: Event, deadline: Optional[Deadline] = None) -> bool:
        """send event obj to client"""
        return await self.__call__(Response.from_event(event, self._create_context()), deadline=deadline)

    async def send_exc(self, exc: Exception, deadline: Optional[Deadline] = None) -> bool:
        """send exc obj to client"""
        return await self.__call__(Response.from_exc(exc, self._create_context()), deadline=deadline)
