import asyncio
import inspect
import logging
import msgpack

from functools import wraps
from typing import Any, Callable, cast, List, Optional, Union, Tuple

from rap import exceptions as rap_exc
from rap.aes import Crypto
from rap.conn.connection import Connection
from rap.conn.pool import Pool
from rap.exceptions import (
    AuthError,
    RPCError,
    ProtocolError,
)
from rap.types import (
    REQUEST_TYPE,
    RESPONSE_TYPE,
    BASE_REQUEST_TYPE,
    BASE_RESPONSE_TYPE
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
        conn = await self._client.conn.acquire()
        try:
            msg_id = await self._client._request(
                conn,
                self._method,
                *self._args,
                call_id=self._call_id
            )
            result, call_id = await self._client._response(conn, msg_id)
            # The server will return the call id of the generator function,
            # and the client can continue to get data based on the call id.
            # If no data, the server will return StopAsyncIteration or StopIteration error.
            self._call_id = call_id
            return result
        finally:
            self._client.conn.release(conn)


class Client:

    def __init__(
            self,
            timeout: int = 9,
            secret: Optional[str] = None,
            host: str = 'localhost',
            port: int = 9000,
            ssl_crt_path: Optional[str] = None,
            min_size: Optional[int] = None,
            max_size: Optional[int] = None
    ):
        self._conn: Union[Connection, Pool, None] = None
        self._msg_id: int = 0

        self._timeout: int = timeout
        self._host: str = host
        self._port: int = port
        self._ssl_crt_path: Optional[str] = ssl_crt_path
        self._min_size: Optional[int] = min_size
        self._max_size: Optional[int] = max_size

        if secret is not None:
            self._crypto: 'Crypto' = Crypto(secret)
        else:
            self._crypto: 'Optional[Crypto]' = None

    def close(self):
        """close client conn"""
        if not self._conn or self._conn.is_closed():
            raise RuntimeError('Connection already closed')
        try:
            self._conn.close()
        except AttributeError:
            pass

    async def connect(self, host: str = 'localhost', port: int = 9000, ssl_crt_path: Optional[str] = None):
        """Create connection and connect"""
        if self._conn and not self._conn.is_closed():
            raise ConnectionError(f'Client already connected')
        if self._min_size and self._max_size:
            self._conn = Pool(
                host,
                port,
                msgpack.Unpacker(raw=False, use_list=False),
                self._timeout,
                max_size=self._max_size,
                min_size=self._min_size,
                ssl_crt_path=ssl_crt_path
            )
            await self._conn.connect()
        else:
            self._conn = Connection(
                msgpack.Unpacker(raw=False, use_list=False),
                self._timeout,
                ssl_crt_path=ssl_crt_path
            )
            await self._conn.connect(host, port)
        logging.debug(f"Connection to {self._conn.connection_info}...")
        await self._declare()

    async def _declare(self):
        header: dict = {'client_id': self._crypto.key}
        conn: 'Connection' = await self._conn.acquire()
        raw_body: str = 'mock_fack_msg'
        raw_msg_id = await self._base_request(conn, Constant.DECLARE_REQUEST, header, raw_body)
        response_num, msg_id, header, body = await self._base_response(conn)
        if response_num != Constant.DECLARE_RESPONSE and raw_msg_id != msg_id and body != body:
            raise RPCError('declare error')
        client_id = body.get('client_id')
        if client_id is None:
            raise RPCError('declare error, get client id error')
        if self._crypto is not None:
            self._crypto = Crypto(client_id)
        logging.info('declare success')

    async def _base_request(self, conn: 'Connection', request_num: int, header: dict, body: Any):
        if conn is None or self._conn.is_closed():
            raise ConnectionError('Connection not create')

        msg_id: int = self._msg_id + 1
        self._msg_id = msg_id
        if self._crypto is not None:
            body = self._crypto.encrypt_object(body)

        request: BASE_REQUEST_TYPE = (request_num, msg_id, header, body)
        try:
            await conn.write(request, self._timeout)
            logging.debug(f'send:{request} to {conn.connection_info}')
        except asyncio.TimeoutError as e:
            logging.error(f"send to {conn.connection_info} timeout, drop data:{request}")
            raise e
        except Exception as e:
            raise e
        return msg_id

    async def _base_response(self, conn: 'Connection') -> Tuple[int, int, dict, Any]:
        try:
            response: Optional[BASE_RESPONSE_TYPE] = await conn.read(self._timeout)
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
        try:
            response_num, msg_id, header, body = response
        except ValueError:
            raise ProtocolError(f"Can't parse response:{response}")
        if self._crypto is not None:
            try:
                body = self._crypto.decrypt_object(body)
            except Exception:
                raise ProtocolError(f"Can't decrypt body.")
        return response_num, msg_id, header, body

    async def _request(
            self,
            conn: 'Connection',
            method: str,
            *args: Any,
            call_id: Optional[int] = None
    ) -> int:
        """gen request body and send,return request msg id"""
        if conn is None or self._conn.is_closed():
            raise ConnectionError('Connection not create')

        msg_id: int = self._msg_id + 1
        self._msg_id = msg_id
        call_id = call_id if call_id is not None else msg_id

        if self._crypto is not None:
            request: REQUEST_TYPE = (
                Constant.REQUEST,
                msg_id,
                call_id,
                1,
                self._crypto.encrypt(method),
                self._crypto.encrypt_object(args)
            )
        else:
            request: REQUEST_TYPE = (Constant.REQUEST, msg_id, call_id, 0, method, args)
        try:
            await conn.write(request, self._timeout)
            logging.debug(f'send:{request} to {conn.connection_info}')
        except asyncio.TimeoutError as e:
            logging.error(f"send to {conn.connection_info} timeout, drop data:{request}")
            raise e
        except Exception as e:
            raise e
        return msg_id

    async def _response(self, conn: 'Connection', msg_id: int) -> Tuple[Any, int]:
        """read response data and return rpc result, call id"""
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
        response_msg_id, call_id, result = self._parse_response(response)
        if response_msg_id != msg_id:
            raise RPCError('Invalid Message ID')
        return result, call_id

    async def call_by_text(self, method: str, *args: Any) -> Any:
        """rpc client base call method"""
        conn: 'Connection' = await self._conn.acquire()
        try:
            msg_id = await self._request(conn, method, *args)
            result, _ = await self._response(conn, msg_id)
            return result
        finally:
            self._conn.release(conn)

    async def call(self, func: Callable, *args: Any) -> Any:
        """automatically resolve function names and call call_by_text"""
        return await self.call_by_text(func.__name__, *args)

    async def iterator_call(self, method: str, *args: Any) -> Any:
        """Python-specific generator call"""
        async for result in AsyncIteratorCall(method, self, *args):
            yield result

    def register(self, func: Callable) -> Any:
        """Using this method to decorate a fake function can help you use it better.
        (such as ide completion, ide reconstruction and type hints)"""
        if inspect.iscoroutinefunction(func):
            return self._async_register(func)
        elif inspect.isasyncgenfunction(func):
            return self._async_gen_register(func)

    def _async_register(self, func: Callable):
        """Decorate normal function"""
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            return await self.call_by_text(func.__name__, *args)
        return cast(Callable, wrapper)

    def _async_gen_register(self, func: Callable):
        """Decoration generator function"""
        @wraps(func)
        async def wrapper(*args, **kwargs) -> Any:
            async for result in self.iterator_call(func.__name__, *args):
                yield result
        return cast(Callable, wrapper)

    def _parse_response(self, response: RESPONSE_TYPE) -> Tuple[int, int, Any]:
        """Parse the response data according to the rap protocol,
        if there is an exception, throw a python exception, otherwise return the normal rpc result data"""
        if len(response) != 6 or response[0] != Constant.RESPONSE:
            logging.debug(f'Protocol error, received unexpected data: {response}')
            raise ProtocolError()
        try:
            (_, msg_id, call_id, is_encrypt, error, result) = response
        except Exception:
            logging.debug(f'Protocol error, received unexpected data: {response}')
            raise ProtocolError()
        if is_encrypt:
            try:
                error = self._crypto.decrypt_object(error)
                result = self._crypto.decrypt_object(result)
            except Exception:
                raise AuthError()

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
        return msg_id, call_id, result

    # async with support
    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, *args: Tuple):
        self.close()

    @property
    def conn(self):
        return self._conn
