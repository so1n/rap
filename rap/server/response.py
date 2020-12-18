import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any, Tuple, Optional

from rap.common.conn import ServerConnection
from rap.common.exceptions import BaseRapError, ServerError
from rap.common.types import BASE_RESPONSE_TYPE
from rap.common.utlis import Constant, Event, parse_error


@dataclass()
class ResponseModel(object):
    num: int = Constant.MSG_RESPONSE
    msg_id: int = -1
    header: dict = field(default_factory=lambda: {"status_code": 200})
    body: Any = None


class Response(object):
    def __init__(self, conn: ServerConnection, timeout: Optional[int] = None):
        self._conn: ServerConnection = conn
        self._timeout: Optional[int] = timeout

    @staticmethod
    async def response_handle(resp: ResponseModel) -> BASE_RESPONSE_TYPE:
        if isinstance(resp.body, BaseRapError):
            error_response: Optional[Tuple[str, str]] = parse_error(resp.body)
            resp.header["status_code"] = resp.body.status_code
            response_msg: BASE_RESPONSE_TYPE = (
                Constant.SERVER_ERROR_RESPONSE,
                resp.msg_id,
                resp.header,
                error_response[1],
            )
        elif isinstance(resp.body, Event):
            response_msg: BASE_RESPONSE_TYPE = (Constant.SERVER_EVENT, resp.msg_id, resp.header, resp.body.to_tuple())
        elif resp.body is not None:
            response_msg: BASE_RESPONSE_TYPE = (resp.num, resp.msg_id, resp.header, resp.body)
        else:
            exception: BaseRapError = ServerError("not response data")
            error_response: Optional[Tuple[str, str]] = parse_error(exception)
            resp.header["status_code"] = exception.status_code
            response_msg: BASE_RESPONSE_TYPE = (
                Constant.SERVER_ERROR_RESPONSE,
                resp.msg_id,
                resp.header,
                error_response[1],
            )
        return response_msg

    async def __call__(self, resp: ResponseModel) -> bool:
        if not resp:
            return False
        resp.header["version"] = Constant.VERSION
        resp.header["user_agent"] = Constant.USER_AGENT
        logging.debug(f"resp: %s", resp)

        response_msg: BASE_RESPONSE_TYPE = await self.response_handle(resp)
        try:
            await self._conn.write(response_msg, self._timeout)
            return True
        except asyncio.TimeoutError:
            logging.error(f"response to {self._conn.peer} timeout. body:{resp.body}")
        except Exception as e:
            logging.error(f"response to {self._conn.peer} error: {e}. body:{resp.body}")
        return False
