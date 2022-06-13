import asyncio
from typing import Set

from rap.client import Client
from rap.client.endpoint import LocalEndpoint, PrivatePicker
from rap.client.model import Request
from rap.client.processor.base import BaseProcessor
from rap.common.conn import Connection


class CheckConnProcessor(BaseProcessor):
    def __init__(self) -> None:
        self.conn_set: Set[Connection] = set()

    async def process_request(self, request: Request) -> Request:
        if request.context.conn and request.target.endswith("sync_sum"):
            # block event request
            self.conn_set.add(request.context.conn)
        return request


check_conn_processor: CheckConnProcessor = CheckConnProcessor()
client: Client = Client(
    "example", endpoint=LocalEndpoint({"ip": "localhost", "port": 9000}, min_poll_size=1, max_pool_size=1)
)
client.load_processor([check_conn_processor])


# in register, must use async def...
@client.register(picker_class=PrivatePicker)
async def sync_sum(a: int, b: int) -> int:
    return 0


async def main() -> None:
    await client.start()
    for _ in range(3):
        assert 3 == await sync_sum(1, 2)


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        format="[%(asctime)s %(levelname)s] %(message)s", datefmt="%y-%m-%d %H:%M:%S", level=logging.DEBUG
    )

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    assert len(check_conn_processor.conn_set) == 3
