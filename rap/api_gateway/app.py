from typing import Any, Dict, List, Optional, Set

from starlette.applications import Starlette

from rap.api_gateway.component import api_exception
from rap.api_gateway.route import route_func, websocket_route_func
from rap.client import Client


def create_app(
    prefix: str,
    rap_client_list: List[Client],
    private_filter: Optional[bool] = None,
    group_filter: Optional[Set[str]] = None,
) -> Starlette:
    """
    prefix: api url prefix
    rap_client_list: rap client list
    private_filter: Can access the function whose attribute is_private is true
    group_filter: Which groups can be accessed
    """
    app: Starlette = Starlette()
    func_info_dict: Dict[str, Dict[str, Any]] = {}
    rap_client_dict: Dict[str, Client] = {rap_client.server_name: rap_client for rap_client in rap_client_list}
    app.add_exception_handler(Exception, api_exception)
    app.state.rap_client_dict = rap_client_dict
    app.state.private_filter = private_filter
    app.state.group_filter = group_filter
    app.state.func_info_dict = func_info_dict

    @app.on_event("startup")
    async def connect() -> None:
        for server_name, rap_client in rap_client_dict.items():
            if rap_client.is_close:
                await rap_client.start()  # type: ignore
            func_list: List[Dict[str, Any]] = await rap_client.raw_invoke("list", group="registry")
            for func_dict in func_list:
                group: str = func_dict["group"]
                func_name: str = func_dict["func_name"]
                func_type: str = func_dict["func_type"]
                func_info_dict[f"{server_name}:{func_type}:{group}:{func_name}"] = func_dict

    @app.on_event("shutdown")
    async def disconnect() -> None:
        for server_name, rap_client in rap_client_dict.items():
            if not rap_client.is_close:
                await rap_client.stop()  # type: ignore

    app.add_route(f"{prefix}/normal", route_func, ["POST"])
    app.add_websocket_route(f"{prefix}/channel", websocket_route_func)
    return app
