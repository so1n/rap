import asyncio
import logging
from typing import Any

from rap.client import Client
from rap.client.processor.context import Context, ContextProcessor
from rap.common.channel import UserChannel

client: Client = Client()
client.load_processor(ContextProcessor())


class RequestIDLogFilter(logging.Filter):
    """
    Log filter to inject the current request id of the request under `log_record.request_id`
    """

    def filter(self, record: Any) -> Any:
        record.request_id = None
        context: Context = Context()
        if context.request:
            record.request_id = context.request.correlation_id or None
        elif context.channel:
            record.request_id = context.channel.channel_id or None
        return record


# in register, must use async def...
@client.register()
async def async_sum(a: int, b: int) -> int:
    return 0


@client.register_channel()
async def echo_body(channel: UserChannel) -> None:
    cnt: int = 0
    await channel.write(f"ping! {cnt}")
    async for body in channel.iter_body():
        print(body)
        cnt += 1
        await channel.write(f"ping! {cnt}")


async def main() -> None:
    await client.start()
    print(f"async result: {await async_sum(1, 3)}")
    await echo_body()
    await client.stop()


def run_client() -> None:
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())


if __name__ == "__main__":
    import logging

    default_handler: logging.Handler = logging.StreamHandler()
    default_handler.addFilter(RequestIDLogFilter())
    logging.basicConfig(
        format="[%(asctime)s %(levelname)s %(request_id)s] %(message)s",
        datefmt="%y-%m-%d %H:%M:%S",
        level=logging.DEBUG,
        handlers=[default_handler],
    )
    run_client()
