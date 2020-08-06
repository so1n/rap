import asyncio
import inspect
import logging
import msgpack

from functools import wraps
from typing import Any, Callable, cast, Optional, Union, Tuple

from rap import exceptions as rap_exc
from rap.conn.connection import Connection
from rap.conn.pool import Pool
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
            client: 'Client',
            *args: Tuple
    ):
        self._method: str = method
        self._call_id: Optional[int] = None
        self._args = args
        self._client: 'Client' = client

    def __aiter__(self):
        return self

    async def __anext__(self):
        conn = await self._client._conn.acquire()
        try:
            msg_id, call_id = await self._client._request(
                conn,
                self._method,
                *self._args,
                call_id=self._call_id
            )
            if not self._call_id:
                self._call_id = call_id
            value = await self._client._response(conn, msg_id)
            return value
        finally:
            await self._client._conn.release(conn)


class Client:

    def __init__(self, timeout: int = 3):
        self._timeout: int = timeout

        self._conn: Union[Connection, Pool, None] = None
        self._msg_id: int = 0

    def close(self):
        if not self._conn or self._conn.is_closed():
            raise RuntimeError('Connection already closed')
        try:
            self._conn.close()
        except AttributeError:
            pass

    async def connect(self, host: str = 'localhost', port: int = 9000):
        if self._conn and not self._conn.is_closed():
            raise ConnectionError(f'Client already connected')
        self._conn = Connection(
            msgpack.Unpacker(raw=False, use_list=False),
            self._timeout
        )
        await self._conn.connect(host, port)
        logging.debug(f"Connection to {self._conn.connection_info}...")

    async def create_pool(
            self,
            host: str = 'localhost',
            port: int = 9000,
            min_size: int = 1,
            max_size: int = 10
    ):
        if self._conn and not self._conn.is_closed():
            raise ConnectionError(f'Client already connected')
        self._conn = Pool(
            host,
            port,
            msgpack.Unpacker(raw=False, use_list=False),
            self._timeout,
            max_size=max_size,
            min_size=min_size,
        )
        await self._conn.connect()
        logging.debug(f"Connection to {self._conn.connection_info}...")

    async def _request(
            self,
            conn: 'Connection',
            method: str,
            *args: Any,
            call_id: Optional[int] = None
    ) -> Tuple[int, int]:
        msg_id: int = self._msg_id + 1
        self._msg_id = msg_id
        if not call_id:
            call_id = msg_id
        if conn is None or self._conn.is_closed():
            raise ConnectionError('Connection not create')
        request: REQUEST_TYPE = (Constant.REQUEST, msg_id, call_id, 0, method, args)
        try:
            await conn.write(request, self._timeout)
            logging.debug(f'send:{request} to {conn.connection_info}')
        except asyncio.TimeoutError as e:
            logging.error(f"send to {conn.connection_info} timeout, drop data:{request}")
            raise e
        except Exception as e:
            raise e
        return msg_id, call_id

    async def _response(self, conn: 'Connection', msg_id: int) -> Any:
        try:
            response: Optional[RESPONSE_TYPE] = await conn.read(self._timeout)
            logging.debug(f'recv raw data: {response}')
        except asyncio.TimeoutError as e:
            logging.error(f"recv response from {conn.connection_info} timeout")
            conn.set_reader_exc(e)
            raise e
        except Exception as e:
            conn.set_reader_exc(e)
            raise e

        if response is None:
            raise ConnectionError("Connection closed")
        response_msg_id, result = self._parse_response(response)
        if response_msg_id != msg_id:
            raise RPCError('Invalid Message ID')
        return result

    async def call_by_text(self, method: str, *args: Any) -> Any:
        conn: 'Connection' = await self._conn.acquire()
        try:
            msg_id, call_id = await self._request(conn, method, *args)
            return await self._response(conn, msg_id)
        finally:
            await self._conn.release(conn)

    async def call(self, func: Callable, *args: Any) -> Any:
        return await self.call_by_text(func.__name__, *args)

    async def iterator_call(self, method: str, *args: Any) -> Any:
        async for result in AsyncIteratorCall(method, self, *args):
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
        if len(response) != 6 or response[0] != Constant.RESPONSE:
            logging.debug(f'Protocol error, received unexpected data: {response}')
            raise ProtocolError()
        try:
            (_, msg_id, call_id, is_encrypt, error, result) = response
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
