import asyncio
import logging
from typing import Any, Tuple, Optional

from rap.aes import Crypto
from rap.conn.connection import ServerConnection
from rap.exceptions import AuthError
from rap.types import RESPONSE_TYPE
from rap.utlis import Constant


def parse_error(exception: Optional[Exception]) -> Optional[Tuple[str, str]]:
    error_response: Optional[Tuple[str, str]] = None
    if exception:
        error_response = (type(exception).__name__, str(exception))
    return error_response


async def response(
    conn: ServerConnection,
    timeout: int,
    crypto: Optional[Crypto] = None,
    msg_id: Optional[int] = None,
    call_id: Optional[int] = None,
    exception: Optional[Exception] = None,
    result: Optional[Any] = None,
    is_auth: int = True
):
    error_response: Optional[Tuple[str, str]] = parse_error(exception)
    if is_auth == 0 and crypto is None:
        response_msg: RESPONSE_TYPE = (Constant.RESPONSE, msg_id, call_id, 0, error_response, result)
    elif is_auth == 1 and crypto is not None:
        response_msg: RESPONSE_TYPE = (
            Constant.RESPONSE,
            msg_id,
            call_id,
            1,
            crypto.encrypt_object(error_response),
            crypto.encrypt_object(result)
        )
    else:
        error_response: Optional[Tuple[str, str]] = parse_error(AuthError())
        response_msg: RESPONSE_TYPE = (
            Constant.RESPONSE,
            msg_id,
            call_id,
            0,
            error_response,
            None
        )
    try:
        await conn.write(response_msg, timeout)
    except asyncio.TimeoutError:
        logging.error(f"response to {conn.peer} timeout. result:{result}, error:{exception}")
    except Exception as e:
        logging.error(f"response to {conn.peer} error: {e}. result:{result}, error:{exception}")
