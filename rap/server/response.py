import asyncio
import logging
from typing import List, Optional, Tuple

from rap.common.conn import ServerConnection
from rap.common.exceptions import BaseRapError
from rap.common.utlis import Constant, Event, parse_error
from rap.server.model import ResponseModel
from rap.server.processor.base import BaseProcessor


class Response(object):
    def __init__(
        self, conn: ServerConnection, timeout: Optional[int] = None, filter_list: Optional[List[BaseProcessor]] = None
    ):
        self._conn: ServerConnection = conn
        self._timeout: Optional[int] = timeout
        self._filter_list: Optional[List[BaseProcessor]] = filter_list

    @staticmethod
    async def response_handle(resp: ResponseModel) -> ResponseModel:
        resp.header["version"] = Constant.VERSION
        resp.header["user_agent"] = Constant.USER_AGENT
        if isinstance(resp.body, BaseRapError):
            error_response: Optional[Tuple[str, str]] = parse_error(resp.body)
            resp.header["status_code"] = resp.body.status_code
            return ResponseModel(
                Constant.SERVER_ERROR_RESPONSE,
                resp.msg_id,
                resp.func_name,
                resp.method,
                resp.header,
                error_response[1],
            )
        elif isinstance(resp.body, Event):
            return ResponseModel(
                Constant.SERVER_EVENT, resp.msg_id, resp.func_name, resp.method, resp.header, resp.body.to_tuple()
            )
        else:
            return resp

    async def __call__(self, resp: ResponseModel) -> bool:
        if not resp:
            return False

        resp = await self.response_handle(resp)
        for filter_ in reversed(self._filter_list):
            await filter_.process_response(resp)
        logging.debug(f"resp: %s", resp)

        try:
            await self._conn.write(
                (resp.num, resp.msg_id, resp.func_name, resp.method, resp.header, resp.body), self._timeout
            )
            return True
        except asyncio.TimeoutError:
            logging.error(f"response to {self._conn.peer} timeout. body:{resp.body}")
        except Exception as e:
            logging.error(f"response to {self._conn.peer} error: {e}. body:{resp.body}")
        return False
