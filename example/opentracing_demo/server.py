import asyncio
from typing import Optional

from aredis import StrictRedis  # type: ignore
from jaeger_client import Config, Tracer  # type: ignore
from opentracing.scope_managers.asyncio import AsyncioScopeManager  # type: ignore

from rap.server import Server, UserChannel
from rap.server.plugin.processor.opentracing import TracingProcessor


async def echo_body(channel: UserChannel) -> None:
    cnt: int = 0
    async for body in channel.iter_body():
        await asyncio.sleep(1)
        cnt += 1
        print(cnt, body)
        if cnt > 2:
            break
        await channel.write(f"pong! {cnt}")


async def async_sum(a: int, b: int) -> int:
    await asyncio.sleep(0.01)  # mock io time
    return a + b


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        format="[%(asctime)s %(levelname)s] %(message)s", datefmt="%y-%m-%d %H:%M:%S", level=logging.DEBUG
    )

    loop = asyncio.new_event_loop()
    redis: StrictRedis = StrictRedis.from_url("redis://localhost")
    rpc_server: Server = Server()
    opentracing_config: Config = Config(
        config={
            "sampler": {"type": "const", "param": 1},
            "logging": True,
            "local_agent": {"reporting_host": "127.0.0.1"},
        },
        scope_manager=AsyncioScopeManager(),
        service_name="rap server opentracing example",
    )
    jaeger_tracer: Optional[Tracer] = opentracing_config.initialize_tracer()
    if not jaeger_tracer:
        raise ValueError("tracer must not None")
    rpc_server.load_processor([TracingProcessor(jaeger_tracer)])
    rpc_server.register(async_sum)
    rpc_server.register(echo_body)
    loop.run_until_complete(rpc_server.run_forever())
    jaeger_tracer.close()
