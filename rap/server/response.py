import asyncio
import logging
import uuid
from typing import Any, List, Optional

from rap.common.conn import ServerConnection
from rap.common.utlis import Constant
from rap.server.model import ResponseModel
from rap.server.processor.base import BaseProcessor

__all__ = ["Response"]


class Response(object):
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
    async def response_handle(resp: ResponseModel):
        def set_header_value(header_key: str, header_Value: Any, is_cover: bool = False):
            """set header value"""
            if is_cover:
                resp.header[header_key] = header_Value
            elif header_key not in resp.header:
                resp.header[header_key] = header_Value

        set_header_value("version", Constant.VERSION, is_cover=True)
        set_header_value("user_agent", Constant.USER_AGENT, is_cover=True)
        set_header_value("request_id", str(uuid.uuid4()), is_cover=True)
        set_header_value("status_code", 200)

    async def __call__(self, resp: ResponseModel) -> bool:
        if not resp:
            return False

        await self.response_handle(resp)
        if self._processor_list:
            for processor in reversed(self._processor_list):
                resp = await processor.process_response(resp)
        logging.debug(f"resp: %s", resp)
        try:
            await self._conn.write(resp.to_msg(), self._timeout)
            return True
        except asyncio.TimeoutError:
            raise asyncio.TimeoutError(f"response to {self._conn.peer_tuple} timeout. resp:{resp}")
