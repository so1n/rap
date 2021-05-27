import uvicorn  # type: ignore
from starlette.applications import Starlette

from rap.api_gateway.app import create_app
from rap.client import Client

client: Client = Client()
client.add_conn("localhost", 9000)
app: Starlette = create_app("/api", client)
uvicorn.run(app)
