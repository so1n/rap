import asyncio
import inspect
import logging

import msgpack
import time

from typing import Any, Dict, Callable, Optional, Tuple

from rap.conn.connection import ServerConnection
from rap.exceptions import (
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
    def __init__(self, conn: ServerConnection, timeout: int):
        self._conn: ServerConnection = conn
        self._timeout: int = timeout

    async def response_to_conn(
            self,
            msg_id: Optional[int] = None,
            call_id: Optional[int] = None,
            exception: Optional[Exception] = None,
            result: Optional[Any] = None
    ):
        error_response: Optional[Tuple[str, str]] = None
        if exception:
            error_response = (type(exception).__name__, str(exception))

        response: RESPONSE_TYPE = (Constant.RESPONSE, msg_id, call_id, 0, error_response, result)
        try:
            await self._conn.write(response, self._timeout)
        except asyncio.TimeoutError:
            logging.error(f"response to {self._conn.peer} timeout. result:{result}, error:{exception}")
        except Exception as e:
            logging.error(f"response to {self._conn.peer} error: {e}. result:{result}, error:{exception}")

    @staticmethod
    def _parse_request(request: REQUEST_TYPE) -> Tuple[int, int, Callable, Tuple, str]:
        if len(request) != 6 or request[0] != Constant.REQUEST:
            raise ProtocolError()

        _, msg_id, call_id, is_encrypt, method_name, args = request
        method = _func_dict.get(method_name)
        if not method:
            raise FuncNotFoundError("No such method {}".format(method_name))
        return msg_id, call_id, method, args, method_name

    async def msg_handle(self, request: REQUEST_TYPE):
        logging.debug(f'get request data:{request} from {self._conn.peer}')
        start_time: float = time.time()

        if not isinstance(request, (tuple, list)):
            await self.response_to_conn(None, None, ProtocolError(), None)
            logging.error(f"parse request data: {request} from {self._conn.peer} error")
            return
        try:
            msg_id, call_id, method, args, method_name = self._parse_request(request)
        except Exception:
            await self.response_to_conn(None, None, ProtocolError(), None)
            logging.error(f"parse request data: {request} from {self._conn.peer}  error")
            return

        status: bool = False
        try:
            if call_id in _generator_dict:
                try:
                    ret = _generator_dict[call_id]
                    if inspect.isgenerator(ret):
                        ret = next(ret)
                    elif inspect.isasyncgen(ret):
                        ret = await ret.__anext__()
                    await self.response_to_conn(msg_id, call_id, None, ret)
                except (StopAsyncIteration, StopIteration) as e:
                    del _generator_dict[call_id]
                    await self.response_to_conn(msg_id, call_id, e, None)
            else:
                if asyncio.iscoroutinefunction(method):
                    ret: Any = await asyncio.wait_for(method(*args), self._timeout)
                else:
                    ret: Any = await get_event_loop().run_in_executor(None, method, *args)
                status = True
                if inspect.isgenerator(ret):
                    _generator_dict[call_id] = ret
                    ret = next(ret)
                elif inspect.isasyncgen(ret):
                    _generator_dict[call_id] = ret
                    ret = await ret.__anext__()
                await self.response_to_conn(msg_id, call_id, None, ret)
        except Exception as e:
            logging.error(f"run:{method_name} error:{e}. peer:{self._conn.peer} request:{request}")
            await self.response_to_conn(msg_id, call_id, ServerError('execute func error'), None)

        logging.info(f"Method:{method_name}, time:{time.time() - start_time}, status:{status}")


class Server(object):

    def __init__(
            self,
            host: str = 'localhost',
            port: int = 9000,
            timeout: int = 9
    ):
        self._func_dict: Dict[str, Callable] = dict()

        self._host: str = host
        self._port: int = port
        self._timeout: int = timeout

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
        request_handle = RequestHandle(conn, self._timeout)
        logging.debug(f'new connection: {conn.peer}')
        while not conn.is_closed():
            try:
                request: Optional[REQUEST_TYPE] = await conn.read(self._timeout)
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
