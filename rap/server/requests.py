import asyncio
import inspect
import logging
import time

from dataclasses import dataclass
from typing import Any, Dict, Optional, Set, Tuple

from rap.common.aes import Crypto
from rap.conn.connection import ServerConnection
from rap.common.exceptions import (
    AuthError,
    BaseRapError,
    FuncNotFoundError,
    LifeCycleError,
    ParseError,
    ProtocolError,
    ServerError,
)
from rap.manager.aes_manager import aes_manager
from rap.manager.client_manager import client_manager, ClientModel, LifeCycleEnum
from rap.manager.func_manager import func_manager
from rap.common.utlis import Constant, MISS_OBJECT, get_event_loop, parse_error


@dataclass()
class RequestModel(object):
    request_num: int
    msg_id: int
    header: dict
    body: Any
    conn: ServerConnection


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
            run_timeout: int,
    ):
        self._run_timeout: int = run_timeout
        self._response_num_dict: Dict[int, int] = {
            Constant.DECLARE_REQUEST: Constant.DECLARE_RESPONSE,
            Constant.MSG_REQUEST: Constant.MSG_RESPONSE,
            Constant.DROP_REQUEST: Constant.DROP_RESPONSE
        }
        self._life_cycle_dict: Dict[int, LifeCycleEnum] = {
            Constant.DECLARE_RESPONSE: LifeCycleEnum.msg,
            Constant.MSG_RESPONSE: LifeCycleEnum.msg,
            Constant.DROP_RESPONSE: LifeCycleEnum.drop,
        }
        self.client_model: 'Optional[ClientModel]' = None

    @staticmethod
    def _check_timeout(timestamp: int) -> bool:
        return (int(time.time()) - timestamp) > 60

    def _body_handle(self, body: dict, client_model: ClientModel) -> Optional[Exception]:
        if self._check_timeout(body.get('timestamp', 0)):
            return ServerError('timeout error')
        nonce: str = body.get('nonce', '')
        if nonce in client_model.nonce_set:
            return ServerError('nonce error')
        else:
            client_model.nonce_set.add(nonce)

    async def dispatch(self, request: RequestModel) -> ResultModel:
        logging.debug(f'get request data:{request} from {request.conn.peer}')

        response_num: Optional[int] = self._response_num_dict.get(request.request_num, Constant.SERVER_ERROR_RESPONSE)

        # create result_model
        result_model: 'ResultModel' = ResultModel(response_num=response_num, msg_id=request.msg_id)

        # check type_id
        if request.request_num is Constant.SERVER_ERROR_RESPONSE:
            logging.error(f"parse request data: {request} from {request.conn.peer} error")
            result_model.exception = ServerError('type_id error')
            return result_model

        # check header
        client_id = request.header.get('client_id', None)
        if client_id is None:
            result_model.exception = ProtocolError('header not found client id')
            return result_model

        # check crypto
        if response_num == Constant.DECLARE_RESPONSE:
            crypto: Crypto = aes_manager.get_aed(client_id)
            client_model: 'ClientModel' = ClientModel(crypto=crypto)
        else:
            client_model = client_manager.get_client_model(client_id)
            if client_model is MISS_OBJECT:
                result_model.exception = AuthError('error client id')
                return result_model

        # check handle client_model & client_model from client_manager
        if self.client_model is None:
            self.client_model = client_model
        elif self.client_model != client_model:
            raise AuthError('error client id')

        result_model.crypto = client_model.crypto

        # check life_cycle
        if not client_model.modify_life_cycle(self._life_cycle_dict.get(response_num, MISS_OBJECT)):
            result_model.exception = LifeCycleError()
            return result_model

        client_model.keep_alive_timestamp = int(time.time())

        if type(request.body) is bytes:
            # check crypto
            if client_model.crypto == MISS_OBJECT:
                result_model.exception = AuthError('aes key error')
                return result_model
            try:
                decrypt_body: dict = client_model.crypto.decrypt_object(request.body)
            except Exception:
                result_model.exception = AuthError('decrypt error')
                return result_model

            exception: 'Optional[Exception]' = self._body_handle(decrypt_body, client_model)
            if exception is not None:
                result_model.exception = exception
                return result_model
        else:
            decrypt_body = request.body

        # dispatch
        if response_num == Constant.DECLARE_RESPONSE:
            client_manager.create_client_model(client_model)
            if client_model.crypto is not MISS_OBJECT:
                client_model.crypto = aes_manager.add_aes(client_model.client_id)
            result_model.result = {'client_id': client_model.client_id}
            return result_model
        elif response_num == Constant.MSG_RESPONSE:
            try:
                call_id: int = decrypt_body['call_id']
                method_name: str = decrypt_body['method_name']
                param: str = decrypt_body['param']
            except Exception:
                result_model.exception = ParseError()
                return result_model

            if method_name.startswith('_root_') and request.conn.peer[0] != '127.0.0.1':
                # root func only called by local client
                exc, exc_info = parse_error(FuncNotFoundError())
                result_model.result = {'exc': exc, 'exc_info': exc_info}
            new_call_id, result = await self.msg_handle(call_id, method_name, param, client_model)
            if isinstance(result, Exception):
                exc, exc_info = parse_error(result)
                result_model.result = {'exc': exc, 'exc_info': exc_info}
            else:
                result_model.result = {'call_id': new_call_id, 'method_name': method_name, 'result': result}
            return result_model
        elif request.request_num == Constant.DROP_REQUEST:
            call_id = decrypt_body['call_id']
            result_model.crypto = client_model.crypto
            client_manager.destroy_client_model(client_model.client_id)
            result_model.result = {'call_id': call_id, 'result': 1}
            return result_model

    async def msg_handle(self, call_id: int, method_name: str, param: str, client_model: 'ClientModel'):
        # really request handle

        # TODO middleware before
        start_time: float = time.time()
        status: bool = False
        method = func_manager.func_dict.get(method_name)
        try:
            if call_id in client_model.generator_dict:
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
                    result: Any = await asyncio.wait_for(method(*param), self._run_timeout)
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
        return call_id, result
