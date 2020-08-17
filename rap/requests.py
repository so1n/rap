import asyncio
import inspect
import logging

import msgpack
import time

from typing import Any, Callable, List, Optional, Tuple

from rap.aes import Crypto
from rap.conn.connection import ServerConnection
from rap.exceptions import (
    AuthError,
    FuncNotFoundError,
    ProtocolError,
    ServerError,
)
from rap.manager.func_manager import func_manager
from rap.middleware import (
    BaseConnMiddleware,
    IpLimitMiddleware
)
from rap.response import response_to_conn
from rap.types import (
    READER_TYPE,
    WRITER_TYPE,
    REQUEST_TYPE,
    RESPONSE_TYPE
)
from rap.utlis import Constant, get_event_loop


class Request(object):
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

        method = func_manager.func_dict.get(method_name)
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
        except Exception as e:
            await self.response_to_conn(None, None, ProtocolError(), None)
            logging.error(f"parse request data: {request} from {self._conn.peer}  error:{e}")
            return

        status: bool = False
        try:
            if method_name.startswith('_root_') and self._conn.peer[0] != '127.0.0.1':
                raise FuncNotFoundError
            elif call_id in func_manager.generator_dict:
                try:
                    result = func_manager.generator_dict[call_id]
                    if inspect.isgenerator(result):
                        result = next(result)
                    elif inspect.isasyncgen(result):
                        result = await result.__anext__()
                    await self.response_to_conn(msg_id, call_id, None, result, is_auth=is_encrypt)
                except (StopAsyncIteration, StopIteration) as e:
                    del func_manager.generator_dict[call_id]
                    await self.response_to_conn(msg_id, None, e, None, is_auth=is_encrypt)
            else:
                if asyncio.iscoroutinefunction(method):
                    result: Any = await asyncio.wait_for(method(*args), self._timeout)
                else:
                    result: Any = await get_event_loop().run_in_executor(None, method, *args)

                if inspect.isgenerator(result):
                    call_id = id(result)
                    func_manager.generator_dict[call_id] = result
                    result = next(result)
                elif inspect.isasyncgen(result):
                    call_id = id(result)
                    func_manager.generator_dict[call_id] = result
                    result = await result.__anext__()
                await self.response_to_conn(msg_id, call_id, None, result, is_auth=is_encrypt)
            status = True
        except Exception as e:
            logging.error(f"run:{method_name} error:{e}. peer:{self._conn.peer} request:{request}")
            await self.response_to_conn(msg_id, call_id, ServerError('execute func error'), None, is_auth=is_encrypt)

        logging.info(f"Method:{method_name}, time:{time.time() - start_time}, status:{status}")
