import asyncio
import logging
from typing import Optional

from jaeger_client import Config, Tracer  # type: ignore
from opentracing.scope_managers.asyncio import AsyncioScopeManager  # type: ignore
from opentracing.scope_managers.contextvars import ContextVarsScopeManager  # type: ignore

from rap.client import Client
from rap.client.processor import TracingProcessor
from rap.common.channel import UserChannel

logging.basicConfig(format="[%(asctime)s %(levelname)s] %(message)s", datefmt="%y-%m-%d %H:%M:%S", level=logging.DEBUG)

config: Config = Config(
    config={"sampler": {"type": "const", "param": 1}, "logging": True, "local_agent": {"reporting_host": "127.0.0.1"}},
    scope_manager=AsyncioScopeManager(),
    service_name="rap client opentracing example",
)
tracer: Optional[Tracer] = config.initialize_tracer()
if not tracer:
    raise ValueError("tracer must not None")

client: Client = Client("example")
client.load_processor([TracingProcessor(tracer)])


# in register, must use async def...
@client.register()
async def async_sum(a: int, b: int) -> int:
    return 0


@client.register()
async def not_found() -> None:
    pass


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
    try:
        await not_found()
    except Exception:
        pass
    await echo_body()
    await client.stop()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    import time

    time.sleep(2)
    tracer.close()
