import asyncio
import inspect
import logging
import time

from typing import Any, Callable, Optional, Tuple

from rap.aes import Crypto
from rap.conn.connection import ServerConnection
from rap.exceptions import (
    AuthError,
    BaseRapError,
    FuncNotFoundError,
    ProtocolError,
    ServerError,
)
from rap.manager.func_manager import func_manager
from rap.response import response
from rap.types import REQUEST_TYPE
from rap.utlis import Constant, get_event_loop


class Request(object):
    def __init__(
            self,
            conn: ServerConnection,
            timeout: int,
            run_timeout: int,
            secret: Optional[str] = None
    ):
        self._conn: ServerConnection = conn
        self._timeout: int = timeout
        self._run_timeout: int = run_timeout

        if secret is not None:
            self._crypto = Crypto(secret)
        else:
            self._crypto: Optional[Crypto] = None

    async def response(
            self,
            msg_id: Optional[int] = None,
            call_id: Optional[int] = None,
            exception: Optional[Exception] = None,
            result: Optional[Any] = None,
            is_auth: int = True
    ):
        await response(
            self._conn,
            self._timeout,
            self._crypto,
            msg_id,
            call_id,
            exception=exception,
            result=result,
            is_auth=is_auth
        )

    def _parse_request(self, request: REQUEST_TYPE) -> Tuple[int, int, int, Callable, Tuple, str]:
        if len(request) != 6:
            raise ProtocolError()

        # parse msg
        type_id, msg_id, call_id, is_encrypt, method_name, args = request
        if type_id != Constant.REQUEST:
            raise ProtocolError()

        # encrypt
        if is_encrypt:
            if self._crypto is None:
                raise AuthError('Server not enable auth')
            method_name = self._crypto.decrypt(method_name)
            args = self._crypto.decrypt_object(args)

        method = func_manager.func_dict.get(method_name)
        if not method:
            raise FuncNotFoundError("No such method {}".format(method_name))

        return msg_id, call_id, is_encrypt, method, args, method_name

    async def msg_handle(self, request: REQUEST_TYPE):
        logging.debug(f'get request data:{request} from {self._conn.peer}')

        # check request msg type
        if not isinstance(request, (tuple, list)):
            await self.response(exception=ProtocolError())
            logging.error(f"parse request data: {request} from {self._conn.peer} error")
            return

        # parse request msg
        try:
            msg_id, call_id, is_encrypt, method, args, method_name = self._parse_request(request)
        except Exception as e:
            if isinstance(e, BaseRapError):
                await self.response(exception=e)
            else:
                await self.response(exception=ProtocolError())
                logging.error(f"parse request data: {request} from {self._conn.peer}  error:{e}")
            return

        # really msg handle

        # TODO middleware before
        start_time: float = time.time()
        status: bool = False
        try:
            if method_name.startswith('_root_') and self._conn.peer[0] != '127.0.0.1':
                # root func only called by local client
                raise FuncNotFoundError
            elif call_id in func_manager.generator_dict:
                try:
                    result = func_manager.generator_dict[call_id]
                    if inspect.isgenerator(result):
                        result = next(result)
                    elif inspect.isasyncgen(result):
                        result = await result.__anext__()
                    await self.response(msg_id, call_id, result=result, is_auth=is_encrypt)
                except (StopAsyncIteration, StopIteration) as e:
                    del func_manager.generator_dict[call_id]
                    await self.response(msg_id, exception=e, is_auth=is_encrypt)
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
                await self.response(msg_id, call_id, result=result, is_auth=is_encrypt)
            status = True
        except Exception as e:
            if isinstance(e, BaseRapError):
                await self.response(msg_id, call_id, exception=e, is_auth=is_encrypt)
            else:
                logging.error(f"run:{method_name} error:{e}. peer:{self._conn.peer} request:{request}")
                await self.response(msg_id, call_id, exception=ServerError('execute func error'), is_auth=is_encrypt)

        # TODO middleware after

        logging.info(f"Method:{method_name}, time:{time.time() - start_time}, status:{status}")
