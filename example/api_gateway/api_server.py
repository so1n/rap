import uvicorn  # type: ignore
from starlette.applications import Starlette

from rap.api_gateway.app import create_app
from rap.client import Client

app: Starlette = create_app("/api", Client())
uvicorn.run(app)
