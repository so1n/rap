import asyncio
import inspect
import logging
import msgpack
import time

from dataclasses import dataclass
from functools import wraps
from typing import Any, Callable, cast, Dict, Optional, Tuple

from rap.common import exceptions as rap_exc
from rap.common.conn import Connection
from rap.common.aes import Crypto
from rap.common.exceptions import RPCError, ProtocolError
from rap.common.types import BASE_REQUEST_TYPE, BASE_RESPONSE_TYPE
from rap.common.utlis import Constant, gen_random_str_id, gen_random_time_id


__all__ = ["Client"]


class AsyncIteratorCall:
    """client support async iterator
    """
    def __init__(self, method: str, client: "Client", *args: Tuple):
        self._method: str = method
        self._call_id: Optional[int] = None
        self._args = args
        self._client: "Client" = client

    def __aiter__(self):
        return self

    async def __anext__(self):
        """
        The server will return the call id of the generator function,
        and the client can continue to get data based on the call id.
        If no data, the server will return StopAsyncIteration or StopIteration error.
        """
        response: Response = await self._client.msg_request(self._method, *self._args, call_id=self._call_id)
        self._call_id = response.body['call_id']
        return response.body['result']


@dataclass()
class Response(object):
    num: int
    msg_id: int
    header: dict
    body: Any


class Client:
    def __init__(
        self,
        timeout: int = 9,
        secret_tuple: Optional[Tuple[str, ...]] = None,
        host: str = "localhost",
        port: int = 9000,
        keep_alive_time: int = 1200,
        ssl_crt_path: Optional[str] = None,
    ):
        self._conn: Optional[Connection] = None
        self._msg_id: int = 0
        self._future_dict: Dict[int, asyncio.Future] = {}
        self._listen_future: Optional[asyncio.Future] = None
        self._is_close: bool = True
        self._timeout: int = timeout
        self._host: str = host
        self._port: int = port
        self._keep_alive_time: int = keep_alive_time
        self._ssl_crt_path: Optional[str] = ssl_crt_path

        if secret_tuple is not None:
            self._crypto: "Crypto" = Crypto(secret_tuple[1])
            self._client_id: str = secret_tuple[0]
        else:
            self._crypto: "Optional[Crypto]" = None
            self._client_id: str = gen_random_str_id(8)

    # async with support

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, *args: Tuple):
        await self.wait_close()

    # connect& close

    async def wait_close(self):
        """close client"""
        if not self._conn or self._conn.is_closed():
            raise RuntimeError("conn already closed")
        if self._is_close:
            raise RuntimeError("Client already closed")
        await self._drop_life_cycle()

        # close listen func
        self._is_close = True
        if not self._listen_future.cancelled():
            self._listen_future.cancel()
        self._listen_future = None
        self._conn.close()

    async def connect(self, host: str = "localhost", port: int = 9000, ssl_crt_path: Optional[str] = None):
        """
        Create&conn connection;
        start listen response;
        send declare msg to server
        """
        if self._conn and not self._conn.is_closed():
            raise ConnectionError(f"Client already connected")
        self._conn = Connection(
            msgpack.Unpacker(raw=False, use_list=False),
            self._timeout,
            ssl_crt_path=ssl_crt_path,
        )
        await self._conn.connect(host, port)
        logging.debug(f"Connection to {self._conn.connection_info}...")
        self._is_close = False
        self._listen_future = asyncio.ensure_future(self._listen())
        await self._declare_life_cycle()

    # util
    @staticmethod
    def raise_error(exc_name: str, exc_info: str = ""):
        """raise python exception"""
        exc = getattr(rap_exc, exc_name, None)
        if not exc:
            exc = globals()["__builtins__"][exc_name]
        raise exc(exc_info)

    async def _listen(self):
        """listen server msg"""
        logging.debug(f'listen:{self._conn} start')
        try:
            while not self._is_close:
                await self._base_response()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logging.error(f'listen status:{self._is_close} error: {e}, close conn:{self._conn}')
            if self._conn and not self._conn.is_closed():
                self._conn.close()

    # life cycle
    async def _declare_life_cycle(self):
        """send declare msg and init client id"""
        body = {}
        response = await self._base_request(Constant.DECLARE_REQUEST, {}, body)
        if response.num != Constant.DECLARE_RESPONSE and response.body != body:
            raise RPCError("declare response error")
        client_id = response.body.get("client_id")
        if client_id is None:
            raise RPCError("declare response error, Can not get client id from body")
        if self._crypto is not None:
            self._client_id = client_id
            self._crypto = Crypto(client_id)
        else:
            self._client_id = client_id
        logging.info("declare success")

    async def _drop_life_cycle(self):
        """send drop msg"""
        call_id: str = gen_random_str_id(8)
        response = await self._base_request(Constant.DROP_REQUEST, {}, {"call_id": call_id})
        if response.num != Constant.DROP_RESPONSE and response.body.get("call_id", "") != call_id:
            raise RPCError("drop response error")
        logging.info("drop response success")

    # request&response
    async def _base_request(self, request_num: int, header: dict, body: Any) -> Response:
        if self._conn.is_closed():
            raise ConnectionError("Connection not create")
        msg_id: int = self._msg_id + 1
        self._msg_id = msg_id

        # set header value
        if "client_id" not in header:
            header["client_id"] = self._client_id
        header["version"] = Constant.VERSION
        header["user_agent"] = Constant.USER_AGENT
        if self._crypto is not None:
            if type(body) is not dict:
                body = {"body": body}
            # set crypto param in body
            body["timestamp"] = int(time.time())
            body["nonce"] = gen_random_time_id()
            body = self._crypto.encrypt_object(body)

        request: BASE_REQUEST_TYPE = (request_num, msg_id, header, body)
        try:
            await self._conn.write(request)
            logging.debug(f"send:{request} to {self._conn.connection_info}")
        except asyncio.TimeoutError as e:
            logging.error(f"send to {self._conn.connection_info} timeout, drop data:{request}")
            raise e
        except Exception as e:
            raise e
        try:
            self._future_dict[msg_id] = asyncio.Future()
            return await asyncio.wait_for(self._future_dict[msg_id], self._timeout)
        finally:
            if msg_id in self._future_dict:
                del self._future_dict[msg_id]

    async def _base_response(self):
        """recv server msg handle"""
        try:
            response: Optional[BASE_RESPONSE_TYPE] = await self._conn.read(self._keep_alive_time)
            logging.debug(f"recv raw data: {response}")
        except asyncio.TimeoutError as e:
            logging.error(f"recv response from {self._conn.connection_info} timeout")
            self._conn.set_reader_exc(e)
            raise e
        except Exception as e:
            self._conn.set_reader_exc(e)
            raise e

        if response is None:
            raise ConnectionError("Connection closed")
        # parse response
        try:
            response_num, msg_id, header, body = response
        except ValueError:
            raise ProtocolError(f"Can't parse response:{response}")
        # server error response handle
        if response_num == Constant.SERVER_ERROR_RESPONSE:
            if header.get("user_agent") == Constant.USER_AGENT:
                self.raise_error(body[0], body[1])
            else:
                raise RuntimeError(body[1])

        # body crypto handle
        if self._crypto is not None and type(body) is bytes:
            try:
                body = self._crypto.decrypt_object(body)
            except Exception:
                raise ProtocolError(f"Can't decrypt body.")

        # server event msg handle
        if response_num == Constant.SERVER_EVENT:
            event, event_info = body
            if event == "close conn":
                raise RuntimeError(f"recv close conn event, event info:{event_info}")

        # set msg to future_dict's `future`
        if msg_id not in self._future_dict:
            raise RuntimeError(f"recv msg_id: {msg_id} error, client not request msg id:{msg_id}")
        self._future_dict[msg_id].set_result(Response(response_num, msg_id, header, body))

    async def msg_request(self, method, *args, call_id=-1) -> Response:
        """msg request handle"""
        response: Response = await self._base_request(
            Constant.MSG_REQUEST, {}, {"call_id": call_id, "method_name": method, "param": args}
        )
        if response.num != Constant.MSG_RESPONSE:
            raise RPCError("request num error")
        if response.header.get("status_code", 200) != 200:
            if response.header.get("user_agent") == Constant.USER_AGENT and "exc" in response.body:
                self.raise_error(response.body["exc"], response.body.get("exc_info", ""))
            else:
                raise RuntimeError(response.body.get("ext_info", ""))
        return response

    # register
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

    # client api
    async def raw_call(self, method: str, *args: Any) -> Any:
        """rpc client base call method"""
        response = await self.msg_request(method, *args)
        return response.body['result']

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
