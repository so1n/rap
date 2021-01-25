import asyncio
import logging
import uuid
from typing import Any, List, Optional, Tuple

from rap.common.conn import ServerConnection
from rap.common.exceptions import BaseRapError
from rap.common.utlis import Constant, Event, parse_error
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
    async def response_handle(resp: ResponseModel) -> ResponseModel:
        def set_header_value(header_key: str, header_Value: Any):
            """set header value"""
            if header_key not in resp.header:
                resp.header[header_key] = header_Value

        set_header_value("version", Constant.VERSION)
        set_header_value("user_agent", Constant.USER_AGENT)
        set_header_value("request_id", str(uuid.uuid4()))
        set_header_value("status_code", 200)

        if isinstance(resp.body, BaseRapError):
            error_response: Optional[Tuple[str, str]] = parse_error(resp.body)
            resp.header["status_code"] = resp.body.status_code
            resp.body = error_response[1]
            return resp
        elif isinstance(resp.body, Event):
            return ResponseModel(Constant.SERVER_EVENT, resp.msg_id, resp.func_name, resp.header, resp.body.to_tuple())
        else:
            return resp

    async def __call__(self, resp: ResponseModel) -> bool:
        if not resp:
            return False

        resp = await self.response_handle(resp)
        if self._processor_list:
            for processor in reversed(self._processor_list):
                resp = await processor.process_response(resp)
        logging.debug(f"resp: %s", resp)

        try:
            await self._conn.write((resp.num, resp.msg_id, resp.func_name, resp.header, resp.body), self._timeout)
            return True
        except asyncio.TimeoutError:
            raise asyncio.TimeoutError(f"response to {self._conn.peer_tuple} timeout. resp:{resp}")
