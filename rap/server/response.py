import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Tuple, Optional

from rap.conn.connection import ServerConnection
from rap.common.exceptions import ServerError
from rap.common.types import BASE_RESPONSE_TYPE
from rap.common.utlis import Constant, parse_error


@dataclass()
class ResponseModel(object):
    response_num: int = Constant.MSG_RESPONSE
    msg_id: int = -1
    header: Optional[dict] = None
    result: Optional[dict] = None
    exception: Optional[Exception] = None
    event: Optional[Tuple[str, Any]] = None


async def response(conn: ServerConnection, resp: ResponseModel, timeout: Optional[int] = None):
    if resp.exception is not None:
        error_response: Optional[Tuple[str, str]] = parse_error(resp.exception)
        response_msg: BASE_RESPONSE_TYPE = (Constant.SERVER_ERROR_RESPONSE, resp.msg_id, resp.header, error_response)
    elif resp.result is not None:
        response_msg: BASE_RESPONSE_TYPE = (resp.response_num, resp.msg_id, resp.header, resp.result)
    elif resp.event is not None:
        response_msg: BASE_RESPONSE_TYPE = (Constant.SERVER_EVENT, resp.msg_id, resp.header, resp.event)
    else:
        error_response: Optional[Tuple[str, str]] = parse_error(ServerError('not response'))
        response_msg: BASE_RESPONSE_TYPE = (Constant.SERVER_ERROR_RESPONSE, resp.msg_id, resp.header, error_response)

    try:
        await conn.write(response_msg, timeout)
    except asyncio.TimeoutError:
        logging.error(f"response to {conn.peer} timeout. result:{resp.result}, error:{resp.exception}")
    except Exception as e:
        logging.error(f"response to {conn.peer} error: {e}. result:{resp.result}, error:{resp.exception}")
