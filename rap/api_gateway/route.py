import asyncio
from typing import Any, Dict, List, Optional

from starlette.requests import HTTPConnection, Request
from starlette.responses import JSONResponse
from starlette.websockets import WebSocket, WebSocketDisconnect

from rap.client import Client
from rap.common.exceptions import ChannelError


def before_check(group: str, func_name: str, func_type: str, request: HTTPConnection) -> Optional[dict]:
    key = f"{func_type}:{group}:{func_name}"
    if key not in request.app.state.func_info_dict:
        return {"code": 1, "msg": "Not Found"}

    if request.app.state.group_filter and group in request.app.state.group_filter:
        return {"code": 1, "msg": "Not Found"}
    if (
        request.app.state.private_filter
        and request.app.state.func_info_dict[key]["is_private"] == request.app.state.private_filter
    ):
        return {"code": 1, "msg": "Not Found"}
    return None


async def websocket_route_func(websocket: WebSocket) -> None:
    await websocket.accept()
    rap_client: Client = websocket.app.state.rap_client
    resp_dict: Dict[str, Any] = await websocket.receive_json()
    try:
        group: str = resp_dict["group"]
        func_name: str = resp_dict["func_name"]
    except KeyError as e:
        await websocket.send_json({"code": 1, "msg": f"param error:{e}"})
        await websocket.close()
        return

    result: Optional[dict] = before_check(group, func_name, "channel", websocket)
    if result:
        await websocket.send_json(result)
        await websocket.close()

    try:
        async with rap_client.transport.channel(func_name, group) as channel:
            await websocket.send_json({"code": 0, "data": "accept"})

            async def send() -> None:
                while True:
                    await websocket.send_json({"code": 0, "data": await channel.read_body()})

            async def receive() -> None:
                while True:
                    await channel.write(await websocket.receive_text())

            send_future: asyncio.Future = asyncio.ensure_future(send())
            receive_future: asyncio.Future = asyncio.ensure_future(receive())
            receive_future.add_done_callback(lambda f: channel.set_finish(str(f.exception())))
            try:
                await channel.wait_close()
            except ChannelError:
                pass
            if send_future.cancelled():
                send_future.cancel()
            elif send_future.done():
                send_future.result()
            if receive_future.cancelled():
                receive_future.cancel()
            elif receive_future.done():
                receive_future.result()
            await websocket.send_json({"code": 0, "data": "close"})
    except WebSocketDisconnect:
        pass
    finally:
        await websocket.close()


async def route_func(request: Request) -> JSONResponse:
    rap_client: Client = request.app.state.rap_client
    resp_dict: Dict[str, Any] = await request.json()

    try:
        group: str = resp_dict["group"]
        func_name: str = resp_dict["func_name"]
    except KeyError as e:
        return JSONResponse({"code": 1, "msg": f"param error:{e}"})

    check_result: Optional[dict] = before_check(group, func_name, "normal", request)
    if check_result:
        return JSONResponse(check_result)

    arg_list: List[Any] = resp_dict.get("arg_list", [])
    kwarg_list: Dict[str, Any] = resp_dict.get("kwarg_list", {})
    if not isinstance(arg_list, list) or not isinstance(kwarg_list, dict):
        return JSONResponse({"code": 1, "msg": "param error"})

    result: Any = await rap_client.raw_call(
        func_name,
        arg_param=arg_list,
        kwarg_param=kwarg_list,
        header={key: value for key, value in request.headers.items()},
        group=group,
    )
    return JSONResponse({"code": 0, "data": result})
