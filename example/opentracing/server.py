import asyncio

from aredis import StrictRedis  # type: ignore
from jaeger_client import Config, Tracer  # type: ignore
from opentracing.scope_managers.contextvars import ContextVarsScopeManager  # type: ignore

from rap.server import Server
from rap.server.processor import TracingProcessor


async def async_sum(a: int, b: int) -> int:
    await asyncio.sleep(1)  # mock io time
    return a + b


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        format="[%(asctime)s %(levelname)s] %(message)s", datefmt="%y-%m-%d %H:%M:%S", level=logging.DEBUG
    )

    loop = asyncio.new_event_loop()
    redis: StrictRedis = StrictRedis.from_url("redis://localhost")
    rpc_server = Server()
    opentracing_config: Config = Config(
        config={
            "sampler": {"type": "const", "param": 1},
            "logging": True,
            "local_agent": {"reporting_host": "127.0.0.1"},
        },
        scope_manager=ContextVarsScopeManager(),
        service_name="rap server opentracing example",
    )
    jaeger_tracer: Tracer = opentracing_config.initialize_tracer()
    rpc_server.load_processor([TracingProcessor(jaeger_tracer)])
    rpc_server.register(async_sum)
    loop.run_until_complete(rpc_server.create_server())

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        loop.run_until_complete(rpc_server.await_closed())
