import asyncio
import inspect
import logging

import msgpack
import time

from typing import Any, Dict, Callable, Optional, Tuple

from rap.aes import Crypto
from rap.conn.connection import ServerConnection
from rap.exceptions import (
    AuthError,
    FuncNotFoundError,
    ProtocolError,
    ServerError,
    RegisteredError
)
from rap.types import (
    READER_TYPE,
    WRITER_TYPE,
    REQUEST_TYPE,
    RESPONSE_TYPE
)
from rap.utlis import Constant, get_event_loop

__all__ = ['Server']


_func_dict: Dict[str, Callable] = dict()
_generator_dict: dict = {}


class RequestHandle(object):
    def __init__(
            self,
            conn: ServerConnection,
            timeout: int,
            run_timeout: int,
            secret: Optional[str] = None):
        self._conn: ServerConnection = conn
        self._timeout: int = timeout
        self._run_timeout: int = run_timeout
        self._crypto: Optional[Crypto] = None
        if secret is not None:
            self._crypto = Crypto(secret)

    async def response_to_conn(
            self,
            msg_id: Optional[int] = None,
            call_id: Optional[int] = None,
            exception: Optional[Exception] = None,
            result: Optional[Any] = None,
            is_auth: int = True
    ):
        error_response: Optional[Tuple[str, str]] = None
        if exception:
            error_response = (type(exception).__name__, str(exception))

        if is_auth == 0 and self._crypto is None:
            response: RESPONSE_TYPE = (Constant.RESPONSE, msg_id, call_id, 0, error_response, result)
        elif is_auth == 1 and self._crypto is not None:
            response: RESPONSE_TYPE = (
                Constant.RESPONSE,
                msg_id,
                call_id,
                1,
                self._crypto.encrypt_object(error_response),
                self._crypto.encrypt_object(result)
            )
        else:
            raise AuthError()
        try:
            await self._conn.write(response, self._timeout)
        except asyncio.TimeoutError:
            logging.error(f"response to {self._conn.peer} timeout. result:{result}, error:{exception}")
        except Exception as e:
            logging.error(f"response to {self._conn.peer} error: {e}. result:{result}, error:{exception}")

    def _parse_request(self, request: REQUEST_TYPE) -> Tuple[int, int, int, Callable, Tuple, str]:
        if len(request) != 6 or request[0] != Constant.REQUEST:
            raise ProtocolError()

        _, msg_id, call_id, is_encrypt, method_name, args = request

        if is_encrypt:
            if self._crypto is None:
                raise AuthError('Not enable auth')
            method_name = self._crypto.decrypt(method_name)
            args = self._crypto.decrypt_object(args)

        method = _func_dict.get(method_name)
        if not method:
            raise FuncNotFoundError("No such method {}".format(method_name))
        return msg_id, call_id, is_encrypt, method, args, method_name

    async def msg_handle(self, request: REQUEST_TYPE):
        logging.debug(f'get request data:{request} from {self._conn.peer}')
        start_time: float = time.time()

        if not isinstance(request, (tuple, list)):
            await self.response_to_conn(None, None, ProtocolError(), None)
            logging.error(f"parse request data: {request} from {self._conn.peer} error")
            return
        try:
            msg_id, call_id, is_encrypt, method, args, method_name = self._parse_request(request)
        except Exception:
            await self.response_to_conn(None, None, ProtocolError(), None)
            logging.error(f"parse request data: {request} from {self._conn.peer}  error")
            return

        status: bool = False
        try:
            if call_id in _generator_dict:
                try:
                    result = _generator_dict[call_id]
                    if inspect.isgenerator(result):
                        result = next(result)
                    elif inspect.isasyncgen(result):
                        result = await result.__anext__()
                    await self.response_to_conn(msg_id, call_id, None, result, is_auth=is_encrypt)
                except (StopAsyncIteration, StopIteration) as e:
                    del _generator_dict[call_id]
                    await self.response_to_conn(msg_id, None, e, None, is_auth=is_encrypt)
            else:
                if asyncio.iscoroutinefunction(method):
                    result: Any = await asyncio.wait_for(method(*args), self._timeout)
                else:
                    result: Any = await get_event_loop().run_in_executor(None, method, *args)

                if inspect.isgenerator(result):
                    call_id = id(result)
                    _generator_dict[call_id] = result
                    result = next(result)
                elif inspect.isasyncgen(result):
                    call_id = id(result)
                    _generator_dict[call_id] = result
                    result = await result.__anext__()
                await self.response_to_conn(msg_id, call_id, None, result, is_auth=is_encrypt)
            status = True
        except Exception as e:
            logging.error(f"run:{method_name} error:{e}. peer:{self._conn.peer} request:{request}")
            await self.response_to_conn(msg_id, call_id, ServerError('execute func error'), None, is_auth=is_encrypt)

        logging.info(f"Method:{method_name}, time:{time.time() - start_time}, status:{status}")


class Server(object):

    def __init__(
            self,
            host: str = 'localhost',
            port: int = 9000,
            timeout: int = 9,
            keep_alive: int = 1200,
            run_timeout: int = 9,
            secret: Optional[str] = None
    ):
        self._func_dict: Dict[str, Callable] = dict()

        self._host: str = host
        self._port: int = port
        self._timeout: int = timeout
        self._keep_alive: int = keep_alive
        self._run_timeout: int = run_timeout
        self._secret: Optional[str] = secret

    def register(self, func: Optional[Callable], name: Optional[str] = None):
        name: str = name if name else func.__name__
        if inspect.isfunction(func):
            if not hasattr(func, "__call__"):
                raise RegisteredError(f"{name} is not a callable object")
            if func in self._func_dict:
                raise RegisteredError(f"Name {name} has already been used")
            _func_dict[name] = func

    async def create_server(self) -> asyncio.AbstractServer:
        return await asyncio.start_server(self.conn_handle, self._host, self._port)

    async def conn_handle(self, reader: READER_TYPE, writer: WRITER_TYPE):
        conn: ServerConnection = ServerConnection(
            reader,
            writer,
            msgpack.Unpacker(use_list=False),
            self._timeout,
            pack_param={'use_bin_type': False},
        )
        request_handle = RequestHandle(conn, self._timeout, self._run_timeout, secret=self._secret)
        logging.debug(f'new connection: {conn.peer}')
        while not conn.is_closed():
            try:
                request: Optional[REQUEST_TYPE] = await conn.read(self._keep_alive)
            except asyncio.TimeoutError:
                await asyncio.sleep(3)
                logging.error(f"recv data from {conn.peer} timeout...")
                break
            except IOError as e:
                logging.debug(f"close conn:{conn.peer} info:{e}")
                break
            except Exception as e:
                await request_handle.response_to_conn(None, None, ServerError(), None)
                conn.set_reader_exc(e)
                raise e
            await request_handle.msg_handle(request)

        if not conn.is_closed():
            conn.close()
            logging.debug(f"close connection: {conn.peer}")
