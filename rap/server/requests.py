import asyncio
import inspect
import logging
import time

from dataclasses import dataclass
from typing import Any, Callable, Coroutine, Dict, Optional, Tuple

from rap.common.conn import ServerConnection
from rap.common.exceptions import (
    AuthError,
    BaseRapError,
    FuncNotFoundError,
    LifeCycleError,
    ParseError,
    ProtocolError,
    RpcRunTimeError,
)
from rap.common.utlis import (
    Constant,
    Event,
    MISS_OBJECT,
    get_event_loop,
    parse_error,
)
from rap.manager.client_manager import client_manager, ClientModel, LifeCycleEnum
from rap.manager.func_manager import func_manager
from rap.server.response import Response, ResponseModel


@dataclass()
class RequestModel(object):
    num: int
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
                "func": self.declare_life_cycle,
                "life_cycle": LifeCycleEnum.msg,
            },
            Constant.MSG_REQUEST: {
                "func": self.msg_life_cycle,
                "life_cycle": LifeCycleEnum.msg,
            },
            Constant.DROP_REQUEST: {
                "func": self.drop_life_cycle,
                "life_cycle": LifeCycleEnum.drop,
            },
            Constant.CLIENT_EVENT_RESPONSE: {
                "func": self.event,
                "life_cycle": LifeCycleEnum.msg,
            },
        }

    async def dispatch(self, request: RequestModel, response: ResponseModel) -> Optional[ResponseModel]:
        dispatch_dict: dict = self._request_num_dict.get(request.num, None)

        # check client_model
        if request.num == Constant.DECLARE_REQUEST:
            client_model: "ClientModel" = ClientModel()
        else:
            client_id = request.header.get("client_id", None)
            client_model = client_manager.get_client_model(client_id)
            if client_model is MISS_OBJECT:
                response.body = AuthError("The current client id has not been registered")
                return response

        # check life_cycle
        if not client_model.modify_life_cycle(dispatch_dict["life_cycle"]):
            response.body = LifeCycleError()
            return response

        request.client_model = client_model
        return await dispatch_dict["func"](request, response)

    @staticmethod
    async def ping_event(conn: ServerConnection, client_model: ClientModel):
        response: Response = Response()
        while not conn.is_closed():
            diff_time: int = int(time.time()) - client_model.keep_alive_timestamp
            if diff_time > 130:
                event_resp: ResponseModel = ResponseModel(
                    Constant.SERVER_EVENT, body=Event(Constant.EVENT_CLOSE_CONN, "recv pong timeout")
                )
                await response(conn, event_resp)
                if not conn.is_closed():
                    conn.close()
                asyncio.ensure_future(client_manager.async_destroy_client_model(client_model.client_id))
            else:
                ping_response: ResponseModel = ResponseModel(Constant.SERVER_EVENT, body=Event(Constant.PING_EVENT, ""))
                await response(conn, ping_response)
                await asyncio.sleep(60)

    async def declare_life_cycle(self, request: RequestModel, response: ResponseModel) -> ResponseModel:
        client_manager.create_client_model(request.client_model)
        response.body = {"client_id": request.client_model.client_id}
        request.client_model.ping_event_future = asyncio.ensure_future(
            self.ping_event(request.conn, request.client_model)
        )
        return response

    async def msg_life_cycle(self, request: RequestModel, response: ResponseModel) -> ResponseModel:
        try:
            call_id: int = request.body["call_id"]
            method_name: str = request.body["method_name"]
            param: str = request.body["param"]
        except KeyError:
            response.body = ParseError("body miss params")
            return response

        # root func only called by local client
        if method_name.startswith("_root_") and request.header["_host"] != "127.0.0.1":
            response.body = FuncNotFoundError(extra_msg=f"func name: {method_name}")
            return response
        else:
            func: Optional[Callable] = func_manager.func_dict.get(method_name)
            if not func:
                response.body = FuncNotFoundError(extra_msg=f"func name: {method_name}")
                return response

            new_call_id, result = await self.msg_handle(request, call_id, func, param)
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

    @staticmethod
    async def event(request: RequestModel, response: ResponseModel) -> Optional[ResponseModel]:
        event_name: str = request.body[0]
        if event_name == Constant.PONG_EVENT:
            client_id: str = request.header.get("client_id")
            client_manager.get_client_model(client_id).keep_alive_timestamp = int(time.time())
        return None

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
                    return call_id, RpcRunTimeError(f"Call {func.__name__} timeout")
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
