import asyncio

from rap.client.transport.transport import Transport


async def main() -> None:
    transport: Transport = Transport(host="127.0.0.1", port=9000, weight=10)
    await transport.connect()
    await transport.declare()
    assert transport.pick_score == 10.0

    await transport.ping()
    assert transport.pick_score > 0
    await transport.await_close()


def run_client() -> None:
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        format="[%(asctime)s %(levelname)s] %(message)s", datefmt="%y-%m-%d %H:%M:%S", level=logging.DEBUG
    )
    run_client()
