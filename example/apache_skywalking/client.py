import asyncio
import logging

from skywalking import agent, config

from rap.client import Client
from rap.client.processor.apache_skywalking import SkywalkingProcessor
from rap.common.channel import UserChannel

config.init(service_name="rap client service", log_reporter_active=True)

logging.basicConfig(format="[%(asctime)s %(levelname)s] %(message)s", datefmt="%y-%m-%d %H:%M:%S", level=logging.DEBUG)

client: Client = Client()
client.load_processor(SkywalkingProcessor())


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


def run_client():
    agent.start()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    import time

    time.sleep(10)
    agent.stop()


if __name__ == "__main__":
    run_client()
