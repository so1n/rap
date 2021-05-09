import uvicorn  # type: ignore
from starlette.applications import Starlette
from starlette.routing import Route

from example.starlette.route import demo1, demo2, demo3
from example.starlette.client import client


async def start_client() -> None:
    await client.connect()


async def stop_client() -> None:
    await client.await_close()


app: Starlette = Starlette(
    routes=[Route("/api/demo1", demo1), Route("/api/demo2", demo2), Route("/api/demo3", demo3)],
    on_startup=[start_client],
    on_shutdown=[stop_client],
)

if __name__ == "__main__":
    import sys
    uvicorn.run(app)
