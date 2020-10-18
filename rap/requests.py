import asyncio
import inspect
import logging
import time
import random

from dataclasses import dataclass
from enum import auto, Enum
from typing import Any, Optional, Tuple

from rap.aes import Crypto
from rap.conn.connection import ServerConnection
from rap.exceptions import (
    AuthError,
    BaseRapError,
    FuncNotFoundError,
    LifeStateError,
    ProtocolError,
    ServerError,
)
from rap.manager.aes_manager import aes_manager
from rap.manager.client_manager import client_manager
from rap.manager.func_manager import func_manager
from rap.types import BASE_REQUEST_TYPE
from rap.utlis import MISS_OBJECT, get_event_loop


class LifeEnum(Enum):
    new: auto()
    init: auto()
    msg: auto()
    drop: auto()


@dataclass()
class RequestModel(object):
    request_num: int
    msg_id: int
    result: Optional[Tuple] = None
    exception: Optional[Exception] = None


class Request(object):
    def __init__(
            self,
            conn: ServerConnection,
            timeout: int,
            run_timeout: int,
    ):
        self._conn: ServerConnection = conn
        self._timeout: int = timeout
        self._run_timeout: int = run_timeout
        self.crypto: Optional[Crypto] = None
        self._state: 'LifeEnum' = LifeEnum.new

    @staticmethod
    def _request_handle(request: BASE_REQUEST_TYPE) -> Tuple[int, int, Any]:
        try:
            request_num, msg_id, result = request
            return request_num, msg_id, result
        except Exception:
            raise ProtocolError()

    def _gen_client_id(self) -> str:
        return f'{self._conn.connection_info}_{str(random.randrange(1000, 9999))}'

    async def dispatch(self, request: BASE_REQUEST_TYPE):
        logging.debug(f'get request data:{request} from {self._conn.peer}')
        if not isinstance(request, (tuple, list)):
            raise ProtocolError()

        request_num, msg_id, result = self._request_handle(request)

        type_id: int = request[0]
        if type_id == 10:
            if self._state != LifeEnum.new:
                raise LifeStateError()
            self._state = LifeEnum.init

            client_id: str = self._gen_client_id()

            # init crypto and encrypt msg
            key, msg = result
            crypto: Crypto = aes_manager.get_aed(key)
            if crypto == MISS_OBJECT:
                raise AuthError('aes key error')
            try:
                msg: str = crypto.decrypt(result)
            except Exception:
                raise AuthError('decrypt error')
            self.crypto = crypto

            client_manager.create_client_info()
            return RequestModel(request_num=11, msg_id=msg_id, result=(client_id, msg))
        elif type_id == 20:
            if self._state == LifeEnum.init:
                self._state = LifeEnum.msg
            if self._state != LifeEnum.msg:
                raise LifeStateError()

            call_id, client_id, method_name, param = self.crypto.encrypt_object(result)
            result: Any = await self.msg_handle(call_id, client_id, method_name, param)
            if isinstance(result, Exception):
                return RequestModel(request_num=11, msg_id=msg_id, exception=result)
            else:
                return RequestModel(request_num=11, msg_id=msg_id, result=(call_id, client_id, method_name, result))
        elif type_id == 0:
            if self._state == LifeEnum.drop:
                raise ServerError('The life cycle is already a drop')
            self._state = LifeEnum.drop
            call_id, client_id, drop_msg = self.crypto.encrypt_object(result)
            # TODO drop conn
            return RequestModel(request_num=11, msg_id=msg_id, result=(call_id, client_id, 1))
        else:
            logging.error(f"parse request data: {request} from {self._conn.peer} error")
            raise ServerError('type_id error')

    async def msg_handle(self, call_id: str, client_id: str, method_name: str, param: str):
        # really msg handle

        # TODO middleware before
        start_time: float = time.time()
        status: bool = False
        method = func_manager.func_dict.get(method_name)
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
                except (StopAsyncIteration, StopIteration) as e:
                    del func_manager.generator_dict[call_id]
                    result = e
            else:
                if asyncio.iscoroutinefunction(method):
                    result: Any = await asyncio.wait_for(method(*param), self._timeout)
                else:
                    result: Any = await get_event_loop().run_in_executor(None, method, *param)

                if inspect.isgenerator(result):
                    call_id = id(result)
                    func_manager.generator_dict[call_id] = result
                    result = next(result)
                elif inspect.isasyncgen(result):
                    call_id = id(result)
                    func_manager.generator_dict[call_id] = result
                    result = await result.__anext__()
            status = True
        except Exception as e:
            if isinstance(e, BaseRapError):
                result = e
            else:
                logging.error(f"run:{method_name} param:{param} error:{e}. peer:{self._conn.peer}")
                result = ServerError('execute func error')
        # TODO middleware after
        logging.info(f"Method:{method_name}, time:{time.time() - start_time}, status:{status}")
        return result
