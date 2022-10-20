import asyncio
import logging
from typing import Any

from aredis import StrictRedis  # type: ignore

from rap.common.channel import UserChannel
from rap.server import Server
from rap.server.plugin.processor.context import Context, ContextProcessor


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


async def async_sum(a: int, b: int) -> int:
    print(Context().request)
    await asyncio.sleep(1)  # mock io time
    return a + b


async def echo_body(channel: UserChannel) -> None:
    cnt: int = 0
    async for body in channel.iter_body():
        await asyncio.sleep(0.1)
        cnt += 1
        print(cnt, body)
        if cnt > 10:
            break
        await channel.write(f"pong! {cnt}")


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

    loop = asyncio.new_event_loop()
    redis: StrictRedis = StrictRedis.from_url("redis://localhost")
    rpc_server: Server = Server()
    rpc_server.register(async_sum)
    rpc_server.register(echo_body)
    rpc_server.load_processor([ContextProcessor()])
    loop.run_until_complete(rpc_server.run_forever())
