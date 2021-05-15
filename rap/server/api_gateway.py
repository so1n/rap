import inspect
import logging
from typing import Any, Dict, List, Optional

from starlette.applications import Starlette
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.types import ASGIApp

from rap.client import Client
from rap.common.exceptions import BaseRapError, FuncNotFoundError
from rap.server import Server
from rap.server.registry import FuncModel, RegistryManager


class Middleware(BaseHTTPMiddleware):
    def __init__(
        self,
        app: ASGIApp,
        *,
        registry: RegistryManager,
        rap_client: Client
    ) -> None:
        super().__init__(app)
        self._registry: RegistryManager = registry
        self._rap_client: Client = rap_client

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        request.state.registry = self._registry
        request.state.rap_client = self._rap_client
        return await call_next(request)


async def api_exception(request: Request, exc: Exception) -> JSONResponse:
    if isinstance(exc, BaseRapError):
        return JSONResponse({"code": exc.status_code, "exc": exc.message})
    return JSONResponse({"code": 1, "exc": str(exc)})


async def route_func(request: Request) -> JSONResponse:
    registry: RegistryManager = request.state.registry
    rap_client: Client = request.state.rap_client
    url: str = request.url.path
    try:
        func_type, group, func_name = url.split("/")[-3:]
    except Exception:
        return JSONResponse({"code": 1, "data": "url error"})

    resp_dict: Dict[str, Any] = await request.json()
    arg_list: List[Any] = resp_dict.get("arg_list", [])
    kwarg_list: Dict[str, Any] = resp_dict.get("kwarg_list", {})
    if not isinstance(arg_list, list) or not isinstance(kwarg_list, dict):
        return JSONResponse({"code": 1, "data": "param error"})

    func_key: str = registry.gen_key(group, func_name, func_type)
    if func_key not in registry:
        raise FuncNotFoundError(extra_msg=f"name: {func_name}")

    func_model: FuncModel = registry[func_key]
    if func_model.type_ == "channel":
        return JSONResponse({"code": 1, "data": "Can not call channel func"})
    if inspect.isgenerator(func_model.func) or inspect.isasyncgen(func_model.func):
        return JSONResponse({"code": 1, "data": "Can not call generator func"})

    result: Any = await rap_client.raw_call(
        func_model.name,
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
    rap_server: Server,
    rap_client: Client,
    private_filter: Optional[bool] = None,
    group_filter: Optional[str] = None,
) -> Starlette:
    app: Starlette = Starlette()
    app.add_middleware(Middleware, registry=rap_server.registry, rap_client=rap_client)
    app.add_exception_handler(Exception, api_exception)
    if rap_client.transport.is_close:
        @app.on_event("startup")
        async def connect() -> None:
            await rap_client.connect()  # type: ignore

        @app.on_event("shutdown")
        async def disconnect() -> None:
            await rap_client.await_close()  # type: ignore

    func_list: List[Dict[str, Any]] = rap_server.registry.get_register_func_list()
    for func_dict in func_list:
        group: str = func_dict["group"]
        func_name: str = func_dict["name"]
        func_type: str = func_dict["type"]

        func_key: str = rap_server.registry.gen_key(group, func_name, func_type)
        if func_key not in rap_server.registry:
            raise FuncNotFoundError(extra_msg=f"name: {func_name}")
        func_model: FuncModel = rap_server.registry[func_key]
        if private_filter and func_model.is_private != private_filter:
            continue
        if group_filter and func_model.group != group_filter:
            continue
        url: str = f"{prefix}/{func_type}/{group}/{func_name}"
        app.add_route(url, route_func, ["POST"])
        logging.debug(f"add {url} to api server")
    return app
