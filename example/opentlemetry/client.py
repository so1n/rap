import asyncio
import logging

from opentelemetry import trace
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from rap.client import Client
from rap.client.processor.opentelemetry import OpenTelemetryProcessor
from rap.common.channel import UserChannel

logging.basicConfig(format="[%(asctime)s %(levelname)s] %(message)s", datefmt="%y-%m-%d %H:%M:%S", level=logging.DEBUG)
trace.set_tracer_provider(TracerProvider(resource=Resource.create({SERVICE_NAME: "client.example"})))
jaeger_exporter = JaegerExporter(
    agent_host_name="localhost",
    agent_port=6831,
)
trace.get_tracer_provider().add_span_processor(BatchSpanProcessor(jaeger_exporter))

client: Client = Client()
client.load_processor([OpenTelemetryProcessor()])


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
