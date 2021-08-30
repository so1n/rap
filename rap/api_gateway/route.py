import asyncio
import logging
from typing import Any, Dict, List, Optional

from starlette.requests import HTTPConnection, Request
from starlette.responses import JSONResponse
from starlette.websockets import WebSocket, WebSocketDisconnect, WebSocketState

from rap.client import Client
from rap.common.exceptions import ChannelError
from rap.common.utils import del_future


def before_check(
    server_name: str, group: str, func_name: str, func_type: str, request: HTTPConnection
) -> Optional[dict]:
    key = f"{server_name}:{func_type}:{group}:{func_name}"
    if key not in request.app.state.func_info_dict:
        return {"code": 3, "msg": "Not Found"}

    if request.app.state.group_filter and group in request.app.state.group_filter:
        return {"code": 3, "msg": "Not Found"}
    if (
        request.app.state.private_filter
        and request.app.state.func_info_dict[key]["is_private"] == request.app.state.private_filter
    ):
        return {"code": 3, "msg": "Not Found"}
    return None


async def websocket_route_func(websocket: WebSocket) -> None:
    await websocket.accept()
    rap_client_dict: Dict[str, Client] = websocket.app.state.rap_client_dict
    resp_dict: Dict[str, Any] = await websocket.receive_json()
    try:
        server_name: str = resp_dict["server_name"]
        group: str = resp_dict["group"]
        func_name: str = resp_dict["func_name"]
    except KeyError as e:
        await websocket.send_json({"code": 1, "msg": f"param error:{e}"})
        await websocket.close()
        return

    if server_name not in rap_client_dict:
        await websocket.send_json({"code": 2, "msg": "server name error"})
        await websocket.close()
        return

    result: Optional[dict] = before_check(server_name, group, func_name, "channel", websocket)
    if result:
        await websocket.send_json(result)
        await websocket.close()

    rap_client: Client = rap_client_dict[server_name]
    try:
        async with rap_client.endpoint.picker() as conn:
            async with rap_client.transport.channel(func_name, conn, group) as channel:
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

            def set_finish(f: asyncio.Future) -> None:
                exc: Optional[BaseException] = f.exception()
                if exc:
                    channel.set_exc(exc)
                else:
                    channel.set_success_finish()

            receive_future.add_done_callback(lambda f: set_finish(f))
            try:
                await channel.wait_close()
            except ChannelError:
                pass
            del_future(send_future)
            del_future(receive_future)
    except WebSocketDisconnect:
        pass
    finally:
        if websocket.application_state != WebSocketState.DISCONNECTED:
            await websocket.send_json({"code": 0, "data": "close"})
            await websocket.close()


async def route_func(request: Request) -> JSONResponse:
    rap_client_dict: Dict[str, Client] = request.app.state.rap_client_dict
    resp_dict: Dict[str, Any] = await request.json()
    try:
        server_name: str = resp_dict["server_name"]
        group: str = resp_dict["group"]
        func_name: str = resp_dict["func_name"]
    except KeyError as e:
        return JSONResponse({"code": 1, "msg": f"param error:{e}"})
    if server_name not in rap_client_dict:
        return JSONResponse({"code": 2, "msg": "server name error"})

    check_result: Optional[dict] = before_check(server_name, group, func_name, "normal", request)
    if check_result:
        return JSONResponse(check_result)

    arg_list: List[Any] = resp_dict.get("arg_list", [])
    if not isinstance(arg_list, list):
        return JSONResponse({"code": 1, "msg": "param error"})

    result: Any = await rap_client_dict[server_name].raw_invoke(
        func_name,
        arg_param=arg_list,
        header={key: value for key, value in request.headers.items()},
        group=group,
    )
    return JSONResponse({"code": 0, "data": result})
