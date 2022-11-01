import logging

import uvicorn  # type: ignore
from starlette.applications import Starlette

from rap.api_gateway.app import create_app
from rap.client import Client


def run_api_server() -> None:
    client: Client = Client()
    app: Starlette = create_app("/api", {"title": "example", "client": client})
    uvicorn.run(app, log_level=True)


if __name__ == "__main__":
    logging.basicConfig(
        format="[%(asctime)s %(levelname)s] %(message)s", datefmt="%y-%m-%d %H:%M:%S", level=logging.DEBUG
    )
    run_api_server()
