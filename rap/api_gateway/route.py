import asyncio
import logging
from typing import Any, Dict, List, Optional

from starlette.requests import HTTPConnection, Request
from starlette.responses import JSONResponse
from starlette.websockets import WebSocket, WebSocketDisconnect, WebSocketState

from rap.api_gateway import exception
from rap.client import Client
from rap.common.asyncio_helper import as_first_completed

logger: logging.Logger = logging.getLogger(__name__)


def before_check(
    server_name: str, group: str, func_name: str, func_type: str, request: HTTPConnection
) -> Optional[Exception]:
    key = f"{server_name}:{func_type}:{group}:{func_name}"
    if key not in request.app.state.func_info_dict:
        return exception.NotFoundError()

    if request.app.state.group_filter and group in request.app.state.group_filter:
        return exception.NotFoundError()
    if (
        request.app.state.private_filter
        and request.app.state.func_info_dict[key]["is_private"] == request.app.state.private_filter
    ):
        return exception.NotFoundError()
    return None


async def _websocket_route_func(websocket: WebSocket) -> None:
    await websocket.accept()
    rap_client_dict: Dict[str, Client] = websocket.app.state.rap_client_dict
    resp_dict: Dict[str, Any] = await websocket.receive_json()
    try:
        server_name: str = resp_dict["server_name"]
        group: str = resp_dict["group"]
        func_name: str = resp_dict["func_name"]
    except KeyError as e:
        raise exception.ParamError(extra_msg=str(e))

    if server_name not in rap_client_dict:
        raise exception.ServerNameError()

    result: Optional[Exception] = before_check(server_name, group, func_name, "channel", websocket)
    if result:
        raise result

    rap_client: Client = rap_client_dict[server_name]
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
    rap_client_dict: Dict[str, Client] = request.app.state.rap_client_dict
    resp_dict: Dict[str, Any] = await request.json()
    try:
        server_name: str = resp_dict["server_name"]
        group: str = resp_dict["group"]
        func_name: str = resp_dict["func_name"]
    except KeyError as e:
        raise exception.ParamError(extra_msg=str(e))
    if server_name not in rap_client_dict:
        raise exception.ServerNameError()

    check_result: Optional[Exception] = before_check(server_name, group, func_name, "normal", request)
    if check_result:
        raise check_result

    arg_list: List[Any] = resp_dict.get("arg_list", [])
    if not isinstance(arg_list, list):
        raise exception.ParamError()

    result: Any = await rap_client_dict[server_name].invoke_by_name(
        func_name,
        arg_param=arg_list,
        header={key: value for key, value in request.headers.items()},
        group=group,
    )
    return JSONResponse({"code": 0, "data": result})
