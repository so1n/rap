import asyncio
import logging
import msgpack

from typing import Any, Optional, Tuple

from rap.conn.connection import Connection
from rap.exceptions import (
    RpcRunTimeError,
    RPCError,
    ProtocolError,
)
from rap.types import (
    REQUEST_TYPE,
    RESPONSE_TYPE
)
from rap.utlis import Constant


__all__ = ['Client']


class Client:

    def __init__(
            self,
            host: str = 'localhost',
            port: int = 9000,
            timeout: int = 3
    ):
        self._host: str = host
        self._port: int = port
        self._timeout: int = timeout
        self._connection_info: str = f'{host}:{port}'

        self._conn: Optional[Connection] = None
        self._msg_id: int = 0

    def close(self):
        if not self._conn or self._conn.is_closed():
            raise RuntimeError('Connection already closed')
        try:
            self._conn.close()
        except AttributeError:
            pass

    async def connect(self):
        self._conn = Connection(
            msgpack.Unpacker(raw=False, use_list=False),
            self._timeout
        )
        await self._conn.connect(self._host, self._port)
        logging.debug(f"Connection to {self._connection_info}...")

    async def call(self, method: str, *args: Tuple, _close: bool = False) -> Any:
        msg_id: int = self._msg_id + 1
        self._msg_id = msg_id

        if self._conn is None or self._conn.is_closed():
            raise ConnectionError('Connection not create')
        request: REQUEST_TYPE = (Constant.REQUEST, msg_id, method, args)
        try:
            await self._conn.write(request, self._timeout)
            logging.debug(f'send:{request} to {self._connection_info}')
        except asyncio.TimeoutError as e:
            logging.error(f"send to {self._connection_info} timeout, drop data:{request}")
            raise e
        except Exception as e:
            raise e

        try:
            response: Optional[RESPONSE_TYPE] = await self._conn.read(self._timeout)
            logging.debug(f'recv raw data: {response}')
        except asyncio.TimeoutError as e:
            logging.error(f"recv response from {self._connection_info} timeout")
            self._conn.set_reader_exc(e)
            raise e
        except Exception as e:
            self._conn.set_reader_exc(e)
            raise e

        if response is None:
            raise ConnectionError("Connection closed")

        response_msg_id, result = self._parse_response(response)
        if response_msg_id != msg_id:
            raise RPCError('Invalid Message ID')
        return result

    @staticmethod
    def _parse_response(response: RESPONSE_TYPE) -> Tuple[int, Any]:
        if len(response) != 4 or response[0] != Constant.RESPONSE:
            logging.debug(f'Protocol error, received unexpected data: {response}')
            raise ProtocolError()
        try:
            (_, msg_id, error, result) = response
        except Exception:
            logging.debug(f'Protocol error, received unexpected data: {response}')
            raise ProtocolError()
        if error:
            if len(error) == 2:
                raise RpcRunTimeError(*error)
            else:
                raise RPCError(str(error))
        return msg_id, result

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, *args: Tuple):
        self.close()
