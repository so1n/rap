import uvicorn  # type: ignore
from starlette.applications import Starlette

from example.api_gateway.server import rpc_server
from rap.client import Client
from rap.server.api_gateway import create_app

app: Starlette = create_app("/api", rpc_server, Client())
uvicorn.run(app)
