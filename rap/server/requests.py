import asyncio
import inspect
import logging
import time

from dataclasses import dataclass
from typing import Any, Callable, Coroutine, Dict, Optional

from rap.common.aes import Crypto
from rap.common.exceptions import (
    AuthError,
    BaseRapError,
    FuncNotFoundError,
    LifeCycleError,
    ParseError,
    ProtocolError,
    ServerError,
    RpcRunTimeError,
)
from rap.common.utlis import (
    Constant,
    MISS_OBJECT,
    get_event_loop,
    gen_random_time_id,
    parse_error,
)
from rap.common.conn import ServerConnection
from rap.manager.aes_manager import aes_manager
from rap.manager.client_manager import client_manager, ClientModel, LifeCycleEnum
from rap.manager.func_manager import func_manager
from rap.server.response import ResponseModel


@dataclass()
class RequestModel(object):
    request_num: int
    msg_id: int
    header: dict
    body: Any
    conn: ServerConnection


class Request(object):
    def __init__(self, run_timeout: int):
        self._run_timeout: int = run_timeout
        self._response_num_dict: Dict[int, int] = {
            Constant.DECLARE_REQUEST: Constant.DECLARE_RESPONSE,
            Constant.MSG_REQUEST: Constant.MSG_RESPONSE,
            Constant.DROP_REQUEST: Constant.DROP_RESPONSE,
        }
        self._life_cycle_dict: Dict[int, LifeCycleEnum] = {
            Constant.DECLARE_RESPONSE: LifeCycleEnum.msg,
            Constant.MSG_RESPONSE: LifeCycleEnum.msg,
            Constant.DROP_RESPONSE: LifeCycleEnum.drop,
        }
        self.client_model: "Optional[ClientModel]" = None

    @staticmethod
    def _check_timeout(timestamp: int) -> bool:
        return (int(time.time()) - timestamp) > 60

    def _body_handle(self, body: dict, client_model: ClientModel) -> Optional[Exception]:
        if self._check_timeout(body.get("timestamp", 0)):
            return ServerError("timeout error")
        nonce: str = body.get("nonce", "")
        if nonce in client_model.nonce_set:
            return ServerError("nonce error")
        else:
            client_model.nonce_set.add(nonce)

    async def dispatch(self, request: RequestModel) -> ResponseModel:
        logging.debug(f"get request data:%s from %s", request, request.conn.peer)

        response_num: Optional[int] = self._response_num_dict.get(request.request_num, Constant.SERVER_ERROR_RESPONSE)

        # create result_model
        resp_model: "ResponseModel" = ResponseModel(response_num=response_num, msg_id=request.msg_id)

        # check type_id
        if response_num is Constant.SERVER_ERROR_RESPONSE:
            logging.error(f"parse request data: {request} from {request.conn.peer} error")
            resp_model.exception = ServerError("type_id error")
            return resp_model

        # check header
        client_id = request.header.get("client_id", None)
        if client_id is None:
            resp_model.exception = ProtocolError("Can not found client id from header")
            return resp_model

        # check client_model
        if response_num == Constant.DECLARE_RESPONSE:
            crypto: Crypto = aes_manager.get_crypto(client_id)
            client_model: "ClientModel" = ClientModel(crypto=crypto)
        else:
            client_model = client_manager.get_client_model(client_id)
            if client_model is MISS_OBJECT:
                resp_model.exception = AuthError("The current client id has not been registered")
                return resp_model

        # check life_cycle
        if not client_model.modify_life_cycle(self._life_cycle_dict.get(response_num, MISS_OBJECT)):
            resp_model.exception = LifeCycleError()
            return resp_model

        client_model.keep_alive_timestamp = int(time.time())

        if type(request.body) is bytes:
            # check crypto
            if client_model.crypto == MISS_OBJECT:
                resp_model.exception = AuthError("aes key error")
                return resp_model
            try:
                decrypt_body: dict = client_model.crypto.decrypt_object(request.body)
            except Exception:
                resp_model.exception = AuthError("decrypt body error")
                return resp_model

            exception: "Optional[Exception]" = self._body_handle(decrypt_body, client_model)
            if exception is not None:
                resp_model.exception = exception
                return resp_model
        else:
            decrypt_body = request.body

        # dispatch
        if response_num == Constant.DECLARE_RESPONSE:
            client_manager.create_client_model(client_model)
            if client_model.crypto is not MISS_OBJECT:
                # declare will gen new crypto and replace
                resp_model.result = client_model.crypto.encrypt_object(
                    {"timestamp": int(time.time()), "nonce": gen_random_time_id(), "client_id": client_model.client_id}
                )
                client_model.crypto = aes_manager.add_crypto(client_model.client_id)
            else:
                resp_model.result = {"client_id": client_model.client_id}
        elif response_num == Constant.MSG_RESPONSE:
            try:
                call_id: int = decrypt_body["call_id"]
                method_name: str = decrypt_body["method_name"]
                param: str = decrypt_body["param"]
            except Exception:
                resp_model.exception = ParseError()
                return resp_model

            # root func only called by local client
            if method_name.startswith("_root_") and request.conn.peer[0] != "127.0.0.1":
                resp_model.exception = FuncNotFoundError(extra_msg=f'func name: {method_name}')
                return resp_model
            else:
                method: Optional[Callable] = func_manager.func_dict.get(method_name)
                if not method:
                    resp_model.exception = FuncNotFoundError(extra_msg=f'func name: {method_name}')
                    return resp_model

                new_call_id, result = await self.msg_handle(request.header, call_id, method, param, client_model)
                resp_model.result = {"call_id": new_call_id, "method_name": method_name}
                if isinstance(result, Exception):
                    exc, exc_info = parse_error(result)
                    if request.header.get("user_agent") == Constant.USER_AGENT:
                        resp_model.result.update({"exc": exc, "exc_info": exc_info})
                    else:
                        resp_model.result.update({"exc_info": exc_info})
                else:
                    resp_model.result = {"call_id": new_call_id, "method_name": method_name, "result": result}
        elif request.request_num == Constant.DROP_REQUEST:
            call_id = decrypt_body["call_id"]
            client_manager.destroy_client_model(client_model.client_id)
            resp_model.result = {"call_id": call_id, "result": 1}

        if client_model.crypto is not MISS_OBJECT and type(resp_model.result) is dict:
            resp_model.result.update(dict(timestamp=int(time.time()), nonce=gen_random_time_id()))
            resp_model.result = client_model.crypto.encrypt_object(resp_model.result)
        return resp_model

    async def msg_handle(self, header: dict, call_id: int, method: Callable, param: str, client_model: "ClientModel"):
        # really request handle
        # version: str = header.get("version")
        user_agent: str = header.get("user_agent")
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
                    coroutine: Coroutine = method(*param)
                else:
                    coroutine: Coroutine = get_event_loop().run_in_executor(None, method, *param)

                try:
                    result: Any = await asyncio.wait_for(coroutine, self._run_timeout)
                except asyncio.TimeoutError as e:
                    raise e
                except Exception as e:
                    return call_id, e

                if inspect.isgenerator(result):
                    if user_agent != Constant.USER_AGENT:
                        result = ProtocolError(f"{user_agent} not support generator")
                    else:
                        call_id = id(result)
                        client_model.generator_dict[call_id] = result
                        result = next(result)
                elif inspect.isasyncgen(result):
                    if user_agent != Constant.USER_AGENT:
                        result = ProtocolError(f"{user_agent} not support generator")
                    else:
                        call_id = id(result)
                        client_model.generator_dict[call_id] = result
                        result = await result.__anext__()
        except Exception as e:
            if isinstance(e, BaseRapError):
                result = e
            else:
                logging.exception(f"run:{method} param:{param} error:{e}.")
                result = RpcRunTimeError("execute func error")
        return call_id, result
