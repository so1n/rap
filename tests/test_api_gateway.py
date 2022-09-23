import asyncio
from typing import Generator, Set

import pytest
from starlette.applications import Starlette
from starlette.testclient import TestClient
from uvicorn.config import Config  # type: ignore
from uvicorn.server import Server as AppServer  # type: ignore

from example.api_gateway.api_client import example_websockets_client
from example.api_gateway.server import Server, create_server
from rap.api_gateway.app import create_app
from rap.client import Client
from rap.common.utils import constant


@pytest.fixture()
def create_test_app() -> Generator[Starlette, None, None]:
    client: Client = Client("test")
    app: Starlette = create_app("/api", [client])
    server: Server = create_server("test")

    async def create_rap_server() -> None:
        await server.create_server()

    async def close_rap_server() -> None:
        await server.shutdown()

    app.router.on_startup.insert(0, create_rap_server)
    app.router.on_shutdown.append(close_rap_server)
    yield app


class TestApiGateWay:
    def test_api_normal(self, create_test_app: Starlette) -> None:
        with TestClient(create_test_app) as client:
            resp = client.post(
                "http://localhost:8000/api/normal",
                json={
                    "server_name": "test",
                    "group": "default",
                    "func_name": "sync_sum",
                    "func_type": "normal",
                    "arg_list": [1, 2],
                },
            )
            assert 3 == resp.json()["data"]

    def test_api_channel(self) -> None:
        """starlette sync test client can not support other server"""
        loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()

        async def main() -> None:
            # start rap server
            rap_server: Server = create_server("test")
            await rap_server.create_server()
            # start app server and until start
            client: Client = Client("test")
            app_server: AppServer = AppServer(Config(create_app("/api", [client])))
            asyncio.ensure_future(app_server.serve())
            while True:
                if hasattr(app_server, "servers") and len(app_server.servers) > 0:
                    break
                await asyncio.sleep(0.1)
            # test api
            await example_websockets_client("test")
            # close servers...
            app_server.should_exit = True
            await rap_server.shutdown()

        loop.run_until_complete(main())

    def test_param_error(self, create_test_app: Starlette) -> None:
        with TestClient(create_test_app, raise_server_exceptions=False) as client:
            resp = client.post(
                "http://localhost:8000/api/normal",
                json={"server_name": "test", "group": "default", "func_type": "normal", "arg_list": [1, 2]},
            )
            assert {"code": 1, "msg": "Param error('func_name')"} == resp.json()
            resp = client.post(
                "http://localhost:8000/api/normal",
                json={
                    "server_name": "test",
                    "group": "default",
                    "func_name": "sync_sum",
                    "func_type": "normal",
                    "arg_list": 1,
                },
            )
            assert {"code": 1, "msg": "Param error"} == resp.json()

    def test_not_found(self) -> None:
        group_set: Set[str] = set()
        group_set.add(constant.DEFAULT_GROUP)
        client: Client = Client("test")
        app: Starlette = create_app("/api", [client], group_filter=group_set)
        server: Server = create_server("test")

        async def create_rap_server() -> None:
            await server.create_server()

        async def close_rap_server() -> None:
            await server.shutdown()

        app.router.on_startup.insert(0, create_rap_server)
        app.router.on_shutdown.append(close_rap_server)

        with TestClient(app, raise_server_exceptions=False) as test_client:
            resp = test_client.post(
                "http://localhost:8000/api/normal",
                json={
                    "server_name": "test",
                    "group": "default",
                    "func_name": "sync_sum",
                    "func_type": "normal",
                    "arg_list": [1, 2],
                },
            )
            assert {"code": 3, "msg": "Not Found"} == resp.json()
