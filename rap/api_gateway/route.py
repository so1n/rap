import asyncio
import logging
from typing import Any, Dict, Optional, Set

from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.websockets import WebSocket, WebSocketDisconnect, WebSocketState

from rap.api_gateway import exception
from rap.api_gateway.component import RapClientTypedDict
from rap.client import Client
from rap.common.asyncio_helper import as_first_completed
from rap.common.utils import constant

logger: logging.Logger = logging.getLogger(__name__)


def before_check(
    group: str, func_name: str, func_type: str, rap_client_dict: RapClientTypedDict
) -> Optional[Exception]:
    key = f"{func_type}:{group}:{func_name}"
    func_info_dict: dict = rap_client_dict.get("func_info_dict", {})
    group_filter: Optional[Set[str]] = rap_client_dict.get("group_filter", set())
    private_filter: Optional[bool] = rap_client_dict.get("private_filter", None)
    if key not in func_info_dict:
        return exception.NotFoundError()

    if group_filter and group in group_filter:
        return exception.NotFoundError()
    if private_filter and func_info_dict[key]["is_private"] == private_filter:
        return exception.NotFoundError()
    return None


async def _websocket_route_func(websocket: WebSocket) -> None:
    await websocket.accept()

    server_name: str = websocket.url.path.split("/")[-1]
    rap_client_dict: RapClientTypedDict = websocket.app.state.rap_client_dict[server_name]
    resp_dict: Dict[str, Any] = await websocket.receive_json()
    try:
        group: str = resp_dict["group"]
        func_name: str = resp_dict["func_name"]
    except KeyError as e:
        raise exception.ParamError(extra_msg=str(e))

    result: Optional[Exception] = before_check(group, func_name, constant.CHANNEL, rap_client_dict)
    if result:
        raise result

    rap_client: Client = rap_client_dict["client"]  # type: ignore
    try:
        async with rap_client.endpoint.picker() as transport:
            async with transport.channel(func_name, group) as channel:
                await websocket.send_json({"code": 0, "data": "accept"})

                async def send() -> None:
                    while True:
                        await websocket.send_json({"code": 0, "data": await channel.read_body()})

                async def receive() -> None:
                    try:
                        while True:
                            await channel.write(await websocket.receive_text())
                    except WebSocketDisconnect:
                        websocket.application_state = WebSocketState.DISCONNECTED
                        logging.info("receive client close event, close websocket and rap channel")

                send_future: asyncio.Future = asyncio.ensure_future(send())
                receive_future: asyncio.Future = asyncio.ensure_future(receive())

                await as_first_completed([receive_future, send_future])
    except WebSocketDisconnect:
        pass
    finally:
        if websocket.application_state != WebSocketState.DISCONNECTED:
            await websocket.send_json({"code": 0, "data": "close"})
            await websocket.close()


async def websocket_route_func(websocket: WebSocket) -> None:
    error: Optional[exception.BaseError] = None
    try:
        await _websocket_route_func(websocket)
    except exception.BaseError as e:
        error = e
    except Exception as e:
        logger.exception(f"Websocket handler error:{e}")
        error = exception.BaseError(extra_msg=str(e))
    if error:
        await websocket.send_json(error.dict)
        await websocket.close()


async def route_func(request: Request) -> JSONResponse:
    server_name: str = request.url.path.split("/")[-1]
    rap_client_dict: RapClientTypedDict = request.app.state.rap_client_dict[server_name]
    resp_dict: Dict[str, Any] = await request.json()
    try:
        group: str = resp_dict["group"]
        func_name: str = resp_dict["func_name"]
    except KeyError as e:
        raise exception.ParamError(extra_msg=str(e))

    check_result: Optional[Exception] = before_check(group, func_name, constant.NORMAL, rap_client_dict)
    if check_result:
        raise check_result

    arg_dict: Dict = resp_dict.get("arg_dict", {})
    if not isinstance(arg_dict, dict):
        raise exception.ParamError()

    rap_client: Client = rap_client_dict["client"]  # type: ignore
    result: Any = await rap_client.invoke_by_name(
        func_name,
        param=arg_dict,
        header={key: value for key, value in request.headers.items()},
        group=group,
    )
    return JSONResponse({"code": 0, "data": result})
