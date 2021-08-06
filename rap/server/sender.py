import logging
import random
import uuid
from typing import Any, List, Optional, TYPE_CHECKING

from rap.common.conn import ServerConnection
from rap.common.utils import Constant
from rap.server.model import Event, Response
from rap.server.plugin.processor.base import BaseProcessor

if TYPE_CHECKING:
    from rap.server.core import Server
__all__ = ["Sender"]


class Sender(object):
    def __init__(
        self,
        app: "Server",
        conn: ServerConnection,
        timeout: Optional[int] = None,
        processor_list: Optional[List[BaseProcessor]] = None,
    ):
        self._app: "Server" = app
        self._max_msg_id: int = 65535
        self._msg_id: int = random.randrange(self._max_msg_id)
        self._conn: ServerConnection = conn
        self._timeout: Optional[int] = timeout
        self._processor_list: Optional[List[BaseProcessor]] = processor_list

    @staticmethod
    def header_handle(resp: Response) -> None:
        def set_header_value(header_key: str, header_value: Any, is_cover: bool = False) -> None:
            """set header value"""
            if is_cover:
                resp.header[header_key] = header_value
            elif header_key not in resp.header:
                resp.header[header_key] = header_value

        set_header_value("version", Constant.VERSION, is_cover=True)
        set_header_value("user_agent", Constant.USER_AGENT, is_cover=True)
        set_header_value("request_id", str(uuid.uuid4()), is_cover=resp.msg_type is Constant.CHANNEL_RESPONSE)

    async def __call__(self, resp: Optional[Response]) -> bool:
        if resp is None:
            return False

        resp.conn = self._conn
        self.header_handle(resp)
        if self._processor_list:
            for processor in reversed(self._processor_list):
                resp = await processor.process_response(resp)
        logging.debug(f"resp: %s", resp)
        msg_id: int = self._msg_id + 1
        # Avoid too big numbers
        self._msg_id = msg_id & self._max_msg_id
        await self._conn.write((msg_id, *resp.to_msg()))
        if resp.target.endswith(Constant.EVENT_CLOSE_CONN):
            if not self._conn.is_closed():
                self._conn.close()
        return True

    async def send_event(self, event: Event) -> bool:
        return await self.__call__(Response.from_event(self._app, event))

    async def send_exc(self, exc: Exception) -> bool:
        return await self.__call__(Response.from_exc(self._app, exc))
