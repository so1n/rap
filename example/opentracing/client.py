import asyncio
import logging

from jaeger_client import Config, Tracer  # type: ignore

from rap.client import Client
from rap.client.processor import TracingProcessor

logging.basicConfig(format="[%(asctime)s %(levelname)s] %(message)s", datefmt="%y-%m-%d %H:%M:%S", level=logging.DEBUG)

config: Config = Config(
    config={"sampler": {"type": "const", "param": 1}, "logging": True, "local_agent": {"reporting_host": "127.0.0.1"}},
    service_name="rap client opentracing example",
)
tracer: Tracer = config.initialize_tracer()

client: Client = Client("example")
client.load_processor([TracingProcessor(tracer)])


# in register, must use async def...
@client.register()
async def async_sum(a: int, b: int) -> int:
    pass


async def main() -> None:
    client.add_conn("localhost", 9000)
    await client.start()
    print(f"async result: {await async_sum(1, 3)}")
    await client.stop()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
