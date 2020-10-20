import asyncio
import inspect
import logging
import time

from dataclasses import dataclass
from typing import Any, Optional, Tuple

from rap.aes import Crypto
from rap.conn.connection import ServerConnection
from rap.exceptions import (
    AuthError,
    BaseRapError,
    FuncNotFoundError,
    LifeCycleError,
    ParseError,
    ProtocolError,
    RPCError,
    ServerError,
)
from rap.manager.aes_manager import aes_manager
from rap.manager.client_manager import client_manager, ClientModel, LifeCycleEnum
from rap.manager.func_manager import func_manager
from rap.types import BASE_REQUEST_TYPE
from rap.utlis import Constant, MISS_OBJECT, gen_id, get_event_loop, parse_error


@dataclass()
class ResultModel(object):
    response_num: int
    msg_id: int
    crypto: Optional[Crypto] = None
    result: Optional[dict] = None
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

    @staticmethod
    def _request_handle(request: BASE_REQUEST_TYPE) -> Tuple[int, int, str, bytes]:
        try:
            request_num, msg_id, client_id, result = request
            return request_num, msg_id, client_id, result
        except Exception:
            raise ProtocolError()

    @staticmethod
    def _check_timeout(timestamp: int) -> bool:
        return (int(time.time()) - timestamp) > 60

    def _body_handle(self, body: dict, client_model: ClientModel) -> Optional[Exception]:
        if self._check_timeout(body.get('timestamp', 0)):
            return ServerError('timeout')
        nonce: str = body.get('nonce', '')
        if nonce in client_model.nonce_set:
            return ServerError('nonce error')
        else:
            client_model.nonce_set.add(nonce)

    async def dispatch(self, request: BASE_REQUEST_TYPE):
        logging.debug(f'get request data:{request} from {self._conn.peer}')
        if not isinstance(request, (tuple, list)):
            raise ProtocolError()

        request_num, msg_id, client_id, result = self._request_handle(request)

        request_num: int = request[0]
        if request_num == Constant.INIT_REQUEST:
            # init crypto and encrypt msg
            result_model: 'ResultModel' = ResultModel(
                response_num=Constant.INIT_RESPONSE,
                msg_id=msg_id,
            )
            crypto: Crypto = aes_manager.get_aed(client_id)
            client_model: 'ClientModel' = ClientModel(crypto=crypto)
            if crypto == MISS_OBJECT:
                result_model.exception = AuthError('aes key error')
                return result_model
            try:
                decrypt_result: dict = crypto.decrypt_object(result)
            except Exception:
                result_model.exception = AuthError('decrypt error')
                return result_model
            exception: 'Optional[Exception]' = self._body_handle(decrypt_result, client_model)
            if exception is not None:
                result_model.exception = exception
                return result_model

            if client_model.life_cycle != LifeCycleEnum.init:
                result_model.exception = LifeCycleError()
                return result_model

            client_model.life_cycle = LifeCycleEnum.msg
            client_manager.create_client_model(client_model)

            result_model.crypto = crypto
            result_model.result = {'client': client_model.client_id}
            return result_model
        elif request_num == Constant.MSG_REQUEST:
            result_model: 'ResultModel' = ResultModel(
                response_num=Constant.MSG_RESPONSE,
                msg_id=msg_id,
            )
            client_model = client_manager.get_client_model(client_id)

            if client_model is MISS_OBJECT:
                result_model.exception = RPCError('client_id error')
                return result_model
            if client_model.life_cycle != LifeCycleEnum.msg:
                result_model.exception = LifeCycleError()
                return result_model
            try:
                decrypt_result: dict = client_model.crypto.decrypt_object(result)
            except Exception:
                result_model.exception = AuthError('decrypt error')
                return result_model

            exception: 'Optional[Exception]' = self._body_handle(decrypt_result, client_model)
            if exception is not None:
                result_model.exception = exception
                return result_model

            try:
                call_id: int = decrypt_result['call_id']
                method_name: str = decrypt_result['method_name']
                param: str = decrypt_result['param']
            except Exception:
                result_model.exception = ParseError()
                return result_model

            result: Any = await self.msg_handle(call_id, method_name, param, client_model)
            result_model.crypto = client_model.crypto
            if isinstance(result, Exception):
                exc, exc_info = parse_error(result)
                result_model.result = {'exc': exc, 'exc_info': exc_info}
            else:
                result_model.result = {'call_id': call_id, 'method_name': method_name, 'result': result}
            return result_model
        elif request_num == Constant.DROP_REQUEST:
            result_model: 'ResultModel' = ResultModel(
                response_num=Constant.DROP_RESPONSE,
                msg_id=msg_id,
            )
            client_model = client_manager.get_client_model(client_id)
            try:
                decrypt_result: dict = client_model.crypto.decrypt_object(result)
            except Exception:
                result_model.exception = AuthError('decrypt error')
                return result_model

            exception: 'Optional[Exception]' = self._body_handle(decrypt_result, client_model)
            if exception is not None:
                result_model.exception = exception
                return result_model
            call_id = decrypt_result['call_id']
            if client_model is MISS_OBJECT:
                result_model.exception = RPCError('client_id error')
                return result_model
            if client_model.life_cycle == LifeCycleEnum.drop:
                result_model.exception = ServerError('The life cycle is already a drop')
                return result_model
            client_model.life_cycle = LifeCycleEnum.drop
            result_model.crypto = client_model.crypto
            result_model.result = {'call_id': call_id, 'result': 1}
            return result_model
        else:
            logging.error(f"parse request data: {request} from {self._conn.peer} error")
            return ResultModel(
                response_num=Constant.SERVER_ERROR_RESPONSE,
                msg_id=msg_id,
                exception=ServerError('type_id error')
            )

    async def msg_handle(self, call_id: int, method_name: str, param: str, client_model: 'ClientModel'):
        # really msg handle

        # TODO middleware before
        start_time: float = time.time()
        status: bool = False
        method = func_manager.func_dict.get(method_name)
        try:
            if method_name.startswith('_root_') and self._conn.peer[0] != '127.0.0.1':
                # root func only called by local client
                raise FuncNotFoundError
            elif call_id in client_model.generator_dict:
                try:
                    result = client_model.generator_dict[call_id]
                    if inspect.isgenerator(result):
                        result = next(result)
                    elif inspect.isasyncgen(result):
                        result = await result.__anext__()
                except (StopAsyncIteration, StopIteration) as e:
                    del client_model.generator_dict[call_id]
                    result = e
            else:
                if asyncio.iscoroutinefunction(method):
                    result: Any = await asyncio.wait_for(method(*param), self._timeout)
                else:
                    result: Any = await get_event_loop().run_in_executor(None, method, *param)

                if inspect.isgenerator(result):
                    call_id = id(result)
                    client_model.generator_dict[call_id] = result
                    result = next(result)
                elif inspect.isasyncgen(result):
                    call_id = id(result)
                    client_model.generator_dict[call_id] = result
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
