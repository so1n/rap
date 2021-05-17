from typing import Any, Dict, List, Optional, Set

from starlette.applications import Starlette

from rap.api_gateway.component import api_exception
from rap.api_gateway.route import route_func, websocket_route_func
from rap.client import Client


def create_app(
    prefix: str,
    rap_client: Client,
    private_filter: Optional[bool] = None,
    group_filter: Optional[Set[str]] = None,
) -> Starlette:
    app: Starlette = Starlette()
    func_info_dict: Dict[str, Dict[str, Any]] = {}
    app.add_exception_handler(Exception, api_exception)
    app.state.rap_client = rap_client
    app.state.private_filter = private_filter
    app.state.group_filter = group_filter
    app.state.func_info_dict = func_info_dict

    if rap_client.transport.is_close:

        @app.on_event("startup")
        async def connect() -> None:
            await rap_client.connect()  # type: ignore

        @app.on_event("shutdown")
        async def disconnect() -> None:
            await rap_client.await_close()  # type: ignore

    app.add_route(f"{prefix}/normal", route_func, ["POST"])
    app.add_websocket_route(f"{prefix}/channel", websocket_route_func)

    @app.on_event("startup")
    async def init_route() -> None:
        func_list: List[Dict[str, Any]] = await rap_client.raw_call("list", group="registry")
        for func_dict in func_list:
            group: str = func_dict["group"]
            func_name: str = func_dict["func_name"]
            func_type: str = func_dict["func_type"]
            key = f"{func_type}:{group}:{func_name}"
            func_info_dict[key] = func_dict

    return app
