import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Any, Tuple, Optional

from rap.common.aes import Crypto
from rap.conn.connection import ServerConnection
from rap.common.exceptions import ServerError
from rap.common.types import BASE_RESPONSE_TYPE
from rap.common.utlis import Constant, parse_error, gen_id, MISS_OBJECT


@dataclass()
class Response(object):
    request_num: Optional[int] = None
    msg_id: Optional[int] = None
    exception: Optional[Exception] = None
    result: Optional[Any] = None


async def response(
    conn: ServerConnection,
    timeout: int,
    crypto: Optional[Crypto] = None,
    response_num: int = Constant.MSG_RESPONSE,
    msg_id: int = -1,
    exception: Optional[Exception] = None,
    result: Optional[dict] = None,
    header: Optional[dict] = None,
    event: Optional[Tuple[str, Any]] = None
):
    if exception is not None:
        error_response: Optional[Tuple[str, str]] = parse_error(exception)
        response_msg: BASE_RESPONSE_TYPE = (Constant.SERVER_ERROR_RESPONSE, msg_id, header, error_response)
    elif result is not None:
        result.update(dict(timestamp=int(time.time()), nonce=gen_id(10)))
        response_msg: BASE_RESPONSE_TYPE = (
            response_num, msg_id, header, crypto.encrypt_object(result) if crypto is not MISS_OBJECT else result
        )
    elif event is not None:
        response_msg: BASE_RESPONSE_TYPE = (Constant.SERVER_EVENT, msg_id, header, event)
    else:
        error_response: Optional[Tuple[str, str]] = parse_error(ServerError('not response'))
        response_msg: BASE_RESPONSE_TYPE = (Constant.SERVER_ERROR_RESPONSE, msg_id, header, error_response)

    try:
        await conn.write(response_msg, timeout)
    except asyncio.TimeoutError:
        logging.error(f"response to {conn.peer} timeout. result:{result}, error:{exception}")
    except Exception as e:
        logging.error(f"response to {conn.peer} error: {e}. result:{result}, error:{exception}")
