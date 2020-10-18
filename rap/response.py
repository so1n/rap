import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Tuple, Optional

from rap.aes import Crypto
from rap.conn.connection import ServerConnection
from rap.exceptions import ServerError
from rap.types import BASE_RESPONSE_TYPE


@dataclass()
class Response(object):
    request_num: Optional[int] = None
    msg_id: Optional[int] = None
    exception: Optional[Exception] = None
    result: Optional[Any] = None


def parse_error(exception: Optional[Exception]) -> Optional[Tuple[str, str]]:
    error_response: Optional[Tuple[str, str]] = None
    if exception:
        error_response = (type(exception).__name__, str(exception))
    return error_response


async def response(
    conn: ServerConnection,
    timeout: int,
    crypto: Optional[Crypto] = None,
    request_num: int = 21,
    msg_id: int = -1,
    exception: Optional[Exception] = None,
    result: Optional[tuple] = None
):
    if exception is not None:
        error_response: Optional[Tuple[str, str]] = parse_error(exception)
        response_msg: BASE_RESPONSE_TYPE = (
            request_num, msg_id, crypto.encrypt_object(error_response) if crypto else error_response
        )
    elif result is not None:
        response_msg: BASE_RESPONSE_TYPE = (
            request_num, msg_id, crypto.encrypt_object(result) if crypto else result
        )
    else:
        error_response: Optional[Tuple[str, str]] = parse_error(ServerError('not response'))
        response_msg: BASE_RESPONSE_TYPE = (
            request_num, msg_id, crypto.encrypt_object(error_response) if crypto else error_response
        )

    try:
        await conn.write(response_msg, timeout)
    except asyncio.TimeoutError:
        logging.error(f"response to {conn.peer} timeout. result:{result}, error:{exception}")
    except Exception as e:
        logging.error(f"response to {conn.peer} error: {e}. result:{result}, error:{exception}")
