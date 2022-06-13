import logging

import uvicorn  # type: ignore
from starlette.applications import Starlette

from rap.api_gateway.app import create_app
from rap.client import Client

logging.basicConfig(format="[%(asctime)s %(levelname)s] %(message)s", datefmt="%y-%m-%d %H:%M:%S", level=logging.DEBUG)
client: Client = Client("example")
app: Starlette = create_app("/api", [client])
uvicorn.run(app, log_level=True)
