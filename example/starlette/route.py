from starlette.requests import Request
from starlette.responses import JSONResponse

from example.starlette.client import client, new_async_gen, new_sync_sum
from example.starlette.model import async_gen, sync_sum


async def demo1(request: Request) -> JSONResponse:
    return JSONResponse(
        {
            sync_sum.__name__: await client.invoke(sync_sum, [1, 2]),
            async_gen.__name__: [i async for i in client.iterator_invoke(async_gen, [10])],
        }
    )


async def demo2(request: Request) -> JSONResponse:
    return JSONResponse(
        {new_sync_sum.__name__: await new_sync_sum(1, 2), new_async_gen.__name__: [i async for i in new_async_gen(10)]}
    )


async def demo3(request: Request) -> JSONResponse:
    return JSONResponse({sync_sum.__name__: await sync_sum(1, 2), async_gen.__name__: [i async for i in async_gen(10)]})
