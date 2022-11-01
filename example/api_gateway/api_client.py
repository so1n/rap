import asyncio
import json
from typing import Any

from httpx import AsyncClient, Response
from websockets import connect  # type: ignore


async def example_websockets_client(server_name: str = "example") -> None:
    async with connect(f"ws://localhost:8000/api/channel/{server_name}") as websocket:

        async def send_json(data: dict) -> None:
            await websocket.send(json.dumps(data))

        async def receive_json() -> dict:
            resp: dict = json.loads(await websocket.recv())
            if resp["code"] == 0:
                return resp["data"]
            else:
                raise RuntimeError(resp["msg"])

        await send_json({"group": "default", "func_name": "async_channel"})
        result: Any = await receive_json()
        if result != "accept":
            return
        cnt: int = 0
        while cnt < 3:
            await websocket.send(str(cnt))
            result = await receive_json()
            assert str(cnt) == result
            cnt += 1
        await websocket.close()


async def example_http_client() -> None:
    async with AsyncClient() as client:
        resp: Response = await client.post(
            "http://localhost:8000/api/normal/example",
            json={
                "group": "default",
                "func_name": "sync_sum",
                "func_type": "normal",
                "arg_dict": {"a": 1, "b": 2},
            },
        )
        print(resp.json())
        assert 3 == resp.json()["data"]


async def main() -> None:
    await example_http_client()
    await example_websockets_client()


def run_api_client() -> None:
    asyncio.run(main())


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        format="[%(asctime)s %(levelname)s] %(message)s", datefmt="%y-%m-%d %H:%M:%S", level=logging.DEBUG
    )
    run_api_client()
