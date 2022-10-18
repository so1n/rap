import asyncio
from typing import Set

from rap.client import Client, PoolProvider
from rap.client.endpoint import PrivatePicker
from rap.client.model import Request
from rap.client.processor.base import BaseProcessor
from rap.client.transport.transport import Transport


class CheckConnProcessor(BaseProcessor):
    def __init__(self) -> None:
        self.transport_set: Set[Transport] = set()

    async def process_request(self, request: Request) -> Request:
        if request.context.transport and request.target.endswith("sync_sum"):
            # block event request
            self.transport_set.add(request.context.transport)
        return request


check_conn_processor: CheckConnProcessor = CheckConnProcessor()
client: Client = Client("example", pool_provider=PoolProvider.build(max_pool_size=1, min_pool_size=1))
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
    assert len(check_conn_processor.transport_set) == 3
