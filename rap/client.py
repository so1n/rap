import asyncio
import inspect
import logging
import msgpack
import time

from functools import wraps
from typing import Any, Callable, cast, Optional, Union, Tuple

from rap.common import exceptions as rap_exc
from rap.common.aes import Crypto
from rap.common.exceptions import RPCError, ProtocolError
from rap.common.types import BASE_REQUEST_TYPE, BASE_RESPONSE_TYPE
from rap.common.utlis import Constant, gen_id
from rap.conn.connection import Connection
from rap.conn.pool import Pool


__all__ = ["Client"]


class AsyncIteratorCall:
    def __init__(self, method: str, client: "Client", *args: Tuple):
        self._method: str = method
        self._call_id: Optional[int] = None
        self._args = args
        self._client: "Client" = client

    def __aiter__(self):
        return self

    async def __anext__(self):
        conn = await self._client.conn.acquire()
        try:
            msg_id = await self._client._request(conn, self._method, *self._args, call_id=self._call_id)
            call_id, _, result = await self._client._response(conn, msg_id)
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
        secret_tuple: Optional[Tuple[str, ...]] = None,
        host: str = "localhost",
        port: int = 9000,
        ssl_crt_path: Optional[str] = None,
        min_size: Optional[int] = None,
        max_size: Optional[int] = None,
    ):
        self._conn: Union[Connection, Pool, None] = None
        self._msg_id: int = 0

        self._timeout: int = timeout
        self._host: str = host
        self._port: int = port
        self._ssl_crt_path: Optional[str] = ssl_crt_path
        self._min_size: Optional[int] = min_size
        self._max_size: Optional[int] = max_size

        if secret_tuple is not None:
            self._crypto: "Crypto" = Crypto(secret_tuple[1])
            self._client_id: str = secret_tuple[0]
        else:
            self._crypto: "Optional[Crypto]" = None
            self._client_id: str = gen_id(4)

    async def wait_close(self):
        """close client conn"""
        if not self._conn or self._conn.is_closed():
            raise RuntimeError("Connection already closed")
        await self._drop()
        self._conn.close()

    async def connect(self, host: str = "localhost", port: int = 9000, ssl_crt_path: Optional[str] = None):
        """Create connection and connect"""
        if self._conn and not self._conn.is_closed():
            raise ConnectionError(f"Client already connected")
        if self._min_size and self._max_size:
            self._conn = Pool(
                host,
                port,
                msgpack.Unpacker(raw=False, use_list=False),
                self._timeout,
                max_size=self._max_size,
                min_size=self._min_size,
                ssl_crt_path=ssl_crt_path,
            )
            await self._conn.connect()
        else:
            self._conn = Connection(
                msgpack.Unpacker(raw=False, use_list=False),
                self._timeout,
                ssl_crt_path=ssl_crt_path,
            )
            await self._conn.connect(host, port)
        logging.debug(f"Connection to {self._conn.connection_info}...")
        await self._declare()

    @staticmethod
    def raise_error(exc_name: str, exc_info: str = ""):
        exc = getattr(rap_exc, exc_name, None)
        if not exc:
            exc = globals()["__builtins__"][exc_name]
        raise exc(exc_info)

    async def _declare(self):
        conn: "Connection" = await self._conn.acquire()
        try:
            raw_msg_id = await self._base_request(conn, Constant.DECLARE_REQUEST, {}, {})
            response_num, msg_id, header, body = await self._base_response(conn)
            if response_num != Constant.DECLARE_RESPONSE and raw_msg_id != msg_id and body != body:
                raise RPCError("declare response error")
            client_id = body.get("client_id")
            if client_id is None:
                raise RPCError("declare response error, Can not get client id from body")
            if self._crypto is not None:
                self._client_id = client_id
                self._crypto = Crypto(client_id)
            else:
                self._client_id = client_id
            logging.info("declare success")
        finally:
            self._conn.release(conn)

    async def _drop(self):
        conn: "Connection" = await self._conn.acquire()
        try:
            call_id: str = gen_id(4)
            raw_msg_id = await self._base_request(conn, Constant.DROP_REQUEST, {}, {"call_id": call_id})
            response_num, msg_id, header, body = await self._base_response(conn)
            if response_num != Constant.DROP_RESPONSE and raw_msg_id != msg_id and body.get("call_id", "") != call_id:
                raise RPCError("drop response error")
            logging.info("drop response success")
        finally:
            self._conn.release(conn)

    async def _base_request(self, conn: "Connection", request_num: int, header: dict, body: Any) -> int:
        if conn is None or self._conn.is_closed():
            raise ConnectionError("Connection not create")

        msg_id: int = self._msg_id + 1
        self._msg_id = msg_id

        if "client_id" not in header:
            header["client_id"] = self._client_id
        header["version"] = Constant.VERSION
        header["programming_language"] = Constant.PROGRAMMING_LANGUAGE
        if self._crypto is not None:
            if type(body) is not dict:
                body = {"body": body}
            body["timestamp"] = int(time.time())
            body["nonce"] = gen_id(10)
            body = self._crypto.encrypt_object(body)

        request: BASE_REQUEST_TYPE = (request_num, msg_id, header, body)
        try:
            await conn.write(request)
            logging.debug(f"send:{request} to {conn.connection_info}")
        except asyncio.TimeoutError as e:
            logging.error(f"send to {conn.connection_info} timeout, drop data:{request}")
            raise e
        except Exception as e:
            raise e
        return msg_id

    async def _base_response(self, conn: "Connection") -> Tuple[int, int, dict, Any]:
        try:
            response: Optional[BASE_RESPONSE_TYPE] = await conn.read(self._timeout)
            logging.debug(f"recv raw data: {response}")
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
        if response_num == Constant.SERVER_ERROR_RESPONSE:
            if header.get("programming_language") == Constant.PROGRAMMING_LANGUAGE:
                self.raise_error(body[0], body[1])
            else:
                raise RuntimeError(body[1])
        if self._crypto is not None and type(body) is bytes:
            try:
                body = self._crypto.decrypt_object(body)
            except Exception:
                raise ProtocolError(f"Can't decrypt body.")
        if response_num == Constant.SERVER_EVENT:
            event, event_info = body
            if event == "close conn":
                raise RuntimeError(f"recv close conn event, event info:{event_info}")
        return response_num, msg_id, header, body

    async def _request(self, conn, method, *args, call_id=-1) -> int:
        return await self._base_request(
            conn, Constant.MSG_REQUEST, {}, {"call_id": call_id, "method_name": method, "param": args}
        )

    async def _response(self, conn, raw_msg_id):
        response_num, msg_id, header, body = await self._base_response(conn)
        if response_num != Constant.MSG_RESPONSE or msg_id != raw_msg_id:
            raise RPCError("request num or msg id error")
        if header.get("status_code", 200) != 200:
            if "exc" in body:
                self.raise_error(body["exc"], body.get("exc_info", ""))
            else:
                raise RuntimeError(body.get("ext_info", ""))
        return body["call_id"], body["method_name"], body["result"]

    async def raw_call(self, method: str, *args: Any) -> Any:
        """rpc client base call method"""
        conn: "Connection" = await self._conn.acquire()
        try:
            msg_id = await self._request(conn, method, *args)
            _, _, result = await self._response(conn, msg_id)
            return result
        finally:
            self._conn.release(conn)

    async def call(self, func: Callable, *args: Any) -> Any:
        """automatically resolve function names and call call_by_text"""
        return await self.raw_call(func.__name__, *args)

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
            return await self.raw_call(func.__name__, *args)

        return cast(Callable, wrapper)

    def _async_gen_register(self, func: Callable):
        """Decoration generator function"""

        @wraps(func)
        async def wrapper(*args, **kwargs) -> Any:
            async for result in self.iterator_call(func.__name__, *args):
                yield result

        return cast(Callable, wrapper)

    # async with support
    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, *args: Tuple):
        await self.wait_close()

    @property
    def conn(self):
        return self._conn
