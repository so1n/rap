import asyncio

from aredis import StrictRedis  # type: ignore
from opentelemetry import trace
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from rap.server import Server, UserChannel
from rap.server.plugin.processor.opentelemetry import OpenTelemetryProcessor


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
    trace.set_tracer_provider(TracerProvider(resource=Resource.create({SERVICE_NAME: "server.example"})))
    jaeger_exporter = JaegerExporter(
        agent_host_name="localhost",
        agent_port=6831,
    )
    trace.get_tracer_provider().add_span_processor(BatchSpanProcessor(jaeger_exporter))
    rpc_server.load_processor([OpenTelemetryProcessor()])
    rpc_server.register(async_sum)
    rpc_server.register(echo_body)
    loop.run_until_complete(rpc_server.run_forever())
