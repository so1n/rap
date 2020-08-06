import asyncio
import inspect
import logging
import msgpack

from functools import wraps
from typing import Any, Callable, cast, Optional, Tuple

from rap import exceptions as rap_exc
from rap.conn.connection import Connection
from rap.exceptions import (
    RPCError,
    ProtocolError,
)
from rap.types import (
    REQUEST_TYPE,
    RESPONSE_TYPE
)
from rap.utlis import (
    Constant,
)


__all__ = ['Client']


class AsyncIteratorCall:
    def __init__(
            self,
            method: str,
            request: 'Client._request',
            response: 'Client._response',
            msg_id: int,
            *args: Tuple
    ):
        self._method: str = method
        self._msg_id: int = msg_id
        self._args = args
        self._request: 'Client._request' = request
        self._response: 'Client._response' = response

    def __aiter__(self):
        return self

    async def __anext__(self):
        msg_id = await self._request(self._method, *self._args, msg_id=self._msg_id)
        value = await self._response(msg_id)
        return value


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

    async def _request(self, method: str, *args: Any, msg_id: Optional[int] = None) -> int:
        if not msg_id:
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
        return msg_id

    async def _response(self, msg_id: int) -> Any:
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

    async def call_by_text(self, method: str, *args: Any) -> Any:
        msg_id: int = await self._request(method, *args)
        return await self._response(msg_id)

    async def call(self, func: Callable, *args: Any) -> Any:
        return await self.call_by_text(func.__name__, *args)

    async def iterator_call(self, method: str, *args: Any) -> Any:
        msg_id: int = await self._request(method, *args)
        async for result in AsyncIteratorCall(method, self._request, self._response, msg_id, *args):
            yield result

    def register(self, func: Callable) -> Any:
        if inspect.iscoroutinefunction(func):
            return self.async_register(func)
        elif inspect.isasyncgenfunction(func):
            return self.async_gen_register(func)

    def async_register(self, func: Callable):
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            return await self.call_by_text(func.__name__, *args)
        return cast(Callable, wrapper)

    def async_gen_register(self, func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs) -> Any:
            async for result in self.iterator_call(func.__name__, *args):
                yield result
        return cast(Callable, wrapper)

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
                error_name, error_info = error
                exc = getattr(rap_exc, error_name, None)
                if not exc:
                    exc = globals()['__builtins__'][error[0]]
                raise exc(error_info)
                # raise getattr(__builtins__, error[0])(error[1])
            else:
                raise RPCError(str(error))
        return msg_id, result

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, *args: Tuple):
        self.close()
