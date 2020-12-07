import asyncio
import inspect
import logging
import time

from dataclasses import dataclass
from typing import Any, Callable, Coroutine, Dict, Optional, Tuple

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
    parse_error,
)
from rap.common.conn import ServerConnection
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
    client_model: Optional[ClientModel] = None


class Request(object):
    def __init__(self, run_timeout: int):
        self._run_timeout: int = run_timeout
        self._request_num_dict: Dict[int, dict] = {
            Constant.DECLARE_REQUEST: {
                'func': self.declare_life_cycle,
                'life_cycle': LifeCycleEnum.msg,
                'response_num': Constant.DECLARE_RESPONSE,
            },
            Constant.MSG_REQUEST: {
                'func': self.msg_life_cycle,
                'life_cycle': LifeCycleEnum.msg,
                'response_num': Constant.MSG_RESPONSE,
            },
            Constant.DROP_REQUEST: {
                'func': self.drop_life_cycle,
                'life_cycle': LifeCycleEnum.drop,
                'response_num': Constant.DROP_REQUEST,
            },
            Constant.CLIENT_EVENT_RESPONSE: {
                'func': self.event,
                'life_cycle': LifeCycleEnum.msg,
                'response_num': -1,
            }
        }

    async def dispatch(self, request: RequestModel) -> ResponseModel:
        dispatch_dict: dict = self._request_num_dict.get(request.request_num, None)
        response_num: int = dispatch_dict.get('response_num', Constant.SERVER_ERROR_RESPONSE)

        # create result_model
        response: "ResponseModel" = ResponseModel(response_num=response_num, msg_id=request.msg_id)

        # check type_id
        if response_num is Constant.SERVER_ERROR_RESPONSE:
            logging.error(f"parse request data: {request} from {request.conn.peer} error")
            response.body = ServerError("type_id error")
            return response

        # check client_model
        if response_num == Constant.DECLARE_RESPONSE:
            client_model: "ClientModel" = ClientModel()
        else:
            client_id = request.header.get("client_id", None)
            client_model = client_manager.get_client_model(client_id)
            if client_model is MISS_OBJECT:
                response.body = AuthError("The current client id has not been registered")
                return response

        # check life_cycle
        if not client_model.modify_life_cycle(dispatch_dict['life_cycle']):
            response.body = LifeCycleError()
            return response

        client_model.keep_alive_timestamp = int(time.time())
        request.client_model = client_model
        return await dispatch_dict['func'](request, response)

    @staticmethod
    async def declare_life_cycle(request: RequestModel, response: ResponseModel) -> ResponseModel:
        client_manager.create_client_model(request.client_model)
        response.body = {"client_id": request.client_model.client_id}
        return response

    async def msg_life_cycle(self, request: RequestModel, response: ResponseModel) -> ResponseModel:
        try:
            call_id: int = request.body["call_id"]
            method_name: str = request.body["method_name"]
            param: str = request.body["param"]
        except KeyError:
            response.body = ParseError('body miss params')
            return response

        # root func only called by local client
        if method_name.startswith("_root_") and request.conn.peer[0] != "127.0.0.1":
            response.body = FuncNotFoundError(extra_msg=f'func name: {method_name}')
            return response
        else:
            method: Optional[Callable] = func_manager.func_dict.get(method_name)
            if not method:
                response.body = FuncNotFoundError(extra_msg=f'func name: {method_name}')
                return response

            new_call_id, result = await self.msg_handle(request, call_id, method, param)
            response.body = {"call_id": new_call_id, "method_name": method_name}
            if isinstance(result, Exception):
                exc, exc_info = parse_error(result)
                if request.header.get("user_agent") == Constant.USER_AGENT:
                    response.body.update({"exc": exc, "exc_info": exc_info})
                else:
                    response.body.update({"exc_info": exc_info})
            else:
                response.body["result"] = result
        return response

    @staticmethod
    async def drop_life_cycle(request: RequestModel, response: ResponseModel) -> ResponseModel:
        call_id = request.body["call_id"]
        client_manager.destroy_client_model(request.client_model.client_id)
        response.body = {"call_id": call_id, "result": 1}
        return response

    async def event(self, request: RequestModel, response: ResponseModel) -> ResponseModel:
        pass

    async def msg_handle(self, request: RequestModel, call_id: int, func: Callable, param: str) -> Tuple[int, Any]:
        user_agent: str = request.header.get("user_agent")
        try:
            if call_id in request.client_model.generator_dict:
                try:
                    result = request.client_model.generator_dict[call_id]
                    if inspect.isgenerator(result):
                        result = next(result)
                    elif inspect.isasyncgen(result):
                        result = await result.__anext__()
                except (StopAsyncIteration, StopIteration) as e:
                    del request.client_model.generator_dict[call_id]
                    result = e
            else:
                if asyncio.iscoroutinefunction(func):
                    coroutine: Coroutine = func(*param)
                else:
                    coroutine: Coroutine = get_event_loop().run_in_executor(None, func, *param)

                try:
                    result: Any = await asyncio.wait_for(coroutine, self._run_timeout)
                except asyncio.TimeoutError:
                    return call_id, RpcRunTimeError(f'Call {func.__name__} timeout')
                except Exception as e:
                    raise e

                if inspect.isgenerator(result):
                    if user_agent != Constant.USER_AGENT:
                        result = ProtocolError(f"{user_agent} not support generator")
                    else:
                        call_id = id(result)
                        request.client_model.generator_dict[call_id] = result
                        result = next(result)
                elif inspect.isasyncgen(result):
                    if user_agent != Constant.USER_AGENT:
                        result = ProtocolError(f"{user_agent} not support generator")
                    else:
                        call_id = id(result)
                        request.client_model.generator_dict[call_id] = result
                        result = await result.__anext__()
        except Exception as e:
            if isinstance(e, BaseRapError):
                result = e
            else:
                logging.exception(f"run:{func} param:{param} error:{e}.")
                result = RpcRunTimeError("execute func error")
        return call_id, result
