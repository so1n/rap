import inspect
import logging
from typing import Any, Dict, List, Optional

from starlette.applications import Starlette
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.websockets import WebSocket, WebSocketDisconnect
from starlette.types import ASGIApp

from rap.client import Client
from rap.common.exceptions import BaseRapError


class Middleware(BaseHTTPMiddleware):
    def __init__(
        self,
        app: ASGIApp,
        *,
        rap_client: Client
    ) -> None:
        super().__init__(app)
        self._rap_client: Client = rap_client

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        request.state.rap_client = self._rap_client
        return await call_next(request)


async def api_exception(request: Request, exc: Exception) -> JSONResponse:
    if isinstance(exc, BaseRapError):
        return JSONResponse({"code": exc.status_code, "msg": exc.message})
    return JSONResponse({"code": 1, "msg": str(exc)})


async def websocket_route_func(websocket: WebSocket) -> None:
    await websocket.accept()
    rap_client: Client = websocket.state.rap_client
    url: str = websocket.url.path
    try:
        func_type, group, func_name = url.split("/")[-3:]
    except Exception:
        await websocket.send_json({"code": 1, "msg": "url error"})
        await websocket.close()
        return

    try:
        async with rap_client.transport.channel(func_name, group) as channel:
            while True:
                request_dict = await websocket.receive_json()
                await channel.write(request_dict)
                await websocket.send_json({"code": 0, "data": await channel.read_body()})
    except WebSocketDisconnect:
        await websocket.close()


async def route_func(request: Request) -> JSONResponse:
    rap_client: Client = request.state.rap_client
    url: str = request.url.path
    try:
        func_type, group, func_name = url.split("/")[-3:]
    except Exception:
        return JSONResponse({"code": 1, "msg": "url error"})

    resp_dict: Dict[str, Any] = await request.json()
    arg_list: List[Any] = resp_dict.get("arg_list", [])
    kwarg_list: Dict[str, Any] = resp_dict.get("kwarg_list", {})
    if not isinstance(arg_list, list) or not isinstance(kwarg_list, dict):
        return JSONResponse({"code": 1, "msg": "param error"})

    result: Any = await rap_client.raw_call(
        func_name,
        arg_param=arg_list,
        kwarg_param=kwarg_list,
        header={key: value for key, value in request.headers.items()},
        group=group
    )
    return JSONResponse(
        {
            "code": 0,
            "data": result
        }
    )


def create_app(
    prefix: str,
    rap_client: Client,
    private_filter: Optional[bool] = None,
    group_filter: Optional[str] = None,
) -> Starlette:
    app: Starlette = Starlette()
    app.add_middleware(Middleware, rap_client=rap_client)
    app.add_exception_handler(Exception, api_exception)

    if rap_client.transport.is_close:
        @app.on_event("startup")
        async def connect() -> None:
            await rap_client.connect()  # type: ignore

        @app.on_event("shutdown")
        async def disconnect() -> None:
            await rap_client.await_close()  # type: ignore

    @app.on_event("startup")
    async def init_route() -> None:
        func_list: List[Dict[str, Any]] = await rap_client.raw_call("list", group="registry")
        for func_dict in func_list:
            group: str = func_dict["group"]
            func_name: str = func_dict["name"]
            func_type: str = func_dict["type"]
            is_private: bool = func_dict["is_private"]

            if private_filter and is_private != private_filter:
                continue
            if group_filter and group != group_filter:
                continue
            url: str = f"{prefix}/{func_type}/{group}/{func_name}"
            if func_type == "channel":
                app.add_websocket_route(url, websocket_route_func)
            else:
                app.add_route(url, route_func, ["POST"])
            logging.debug(f"add {url} to api server")
    return app
