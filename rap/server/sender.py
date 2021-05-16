import asyncio
import logging
import uuid
from typing import Any, List, Optional

from rap.common.conn import ServerConnection
from rap.common.utils import Constant
from rap.server.model import Event, Response
from rap.server.processor.base import BaseProcessor

__all__ = ["Sender"]


class Sender(object):
    def __init__(
        self,
        conn: ServerConnection,
        timeout: Optional[int] = None,
        processor_list: Optional[List[BaseProcessor]] = None,
    ):
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
        set_header_value("request_id", str(uuid.uuid4()), is_cover=resp.num is Constant.CHANNEL_RESPONSE)
        set_header_value("status_code", 200)

    async def __call__(self, resp: Optional[Response]) -> bool:
        if resp is None:
            return False

        self.header_handle(resp)
        if self._processor_list:
            for processor in reversed(self._processor_list):
                resp = await processor.process_response(resp)
        logging.debug(f"resp: %s", resp)
        try:
            await self._conn.write(resp.to_msg(), self._timeout)
            return True
        except asyncio.TimeoutError:
            raise asyncio.TimeoutError(f"response to {self._conn.peer_tuple} timeout. resp:{resp}")

    async def send_event(self, event: Event) -> bool:
        return await self.__call__(Response.from_event(event))

    async def send_exc(self, exc: Exception) -> bool:
        return await self.__call__(Response.from_exc(exc))
