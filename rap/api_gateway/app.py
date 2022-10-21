from typing import Any, Dict, List, Optional, Set

from starlette.applications import Starlette

from rap.api_gateway.component import RapClientTypedDict, api_exception
from rap.api_gateway.route import route_func, websocket_route_func
from rap.client import Client


def add_client(
    app: Starlette,
    title: str,
    client: Client,
    prefix: str = "/",
    private_filter: Optional[bool] = None,
    group_filter: Optional[Set[str]] = None,
) -> None:
    """
    :param app: starlette instance
    :param title: The name of the rap client, used to distinguish different rap clients
    :param client: rap client instance
    :param prefix: routing prefix
    :param private_filter: If True, access to private methods is not allowed
    :param group_filter: Which groups can not be accessed
    """
    try:
        app.state.rap_client_dict
    except AttributeError:
        app.state.rap_client_dict = {}
    if title in app.state.rap_client_dict:
        raise ValueError(f"{title} already exists")
    if not prefix.endswith("/"):
        prefix = prefix + "/"

    func_info_dict: dict = {}
    app.state.rap_client_dict[title] = {
        "title": title,
        "client": client,
        "private_filter": private_filter,
        "group_filter": group_filter,
        "func_info_dict": func_info_dict,
    }

    @app.on_event("startup")
    async def connect() -> None:
        if client.is_close:
            await client.start()
        func_list: List[Dict[str, Any]] = await client.invoke_by_name("list", group="registry")
        for func_dict in func_list:
            group: str = func_dict["group"]
            func_name: str = func_dict["func_name"]
            func_type: str = func_dict["func_type"]
            func_info_dict[f"{func_type}:{group}:{func_name}"] = func_dict

    @app.on_event("shutdown")
    async def disconnect() -> None:
        if not client.is_close:
            await client.stop()

    app.add_route(f"{prefix}normal/{title}", route_func, ["POST"])
    app.add_websocket_route(f"{prefix}channel/{title}", websocket_route_func)


def create_app(
    prefix: str,
    rap_client_dict: RapClientTypedDict,
) -> Starlette:
    """
    prefix: api url prefix
    rap_client_dict: For the configuration of different rap clients, see `add client` for details.
    """
    app: Starlette = Starlette()
    add_client(app, prefix=prefix, **rap_client_dict)  # type: ignore
    app.add_exception_handler(Exception, api_exception)
    return app
