from typing import AsyncIterator
import uvicorn

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from rap.client import Client


client: Client = Client()


@client.register()
async def sync_sum(a: int, b: int) -> int: pass


@client.register()
async def async_gen(a: int) -> AsyncIterator[int]: yield


async def start_client():
    await client.connect()


async def stop_client():
    await client.await_close()


async def demo1(request: Request) -> JSONResponse:
    return JSONResponse(
        {
            sync_sum.__name__: await client.call(sync_sum, 1, 2),
            async_gen.__name__: [i async for i in client.iterator_call(async_gen, 10)]
        }
    )


async def demo2(request: Request) -> JSONResponse:
    return JSONResponse(
        {
            sync_sum.__name__: await sync_sum(1, 2),
            async_gen.__name__: [i async for i in async_gen(10)]
        }
    )


app: Starlette = Starlette(
    routes=[
        Route('/api/demo1', demo1),
        Route('/api/demo2', demo2)
    ],
    on_startup=[start_client],
    on_shutdown=[stop_client]
)
uvicorn.run(app)
