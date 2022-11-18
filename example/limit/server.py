import asyncio

from aredis import StrictRedis  # type: ignore

from rap.common.channel import UserChannel
from rap.common.utils import constant
from rap.server import Server
from rap.server.model import Request
from rap.server.plugin.processor import limit


async def demo(a: int, b: int) -> int:
    return a + b


async def demo1(a: int, b: int) -> int:
    return a + b


async def echo_body(channel: UserChannel) -> None:
    cnt: int = 0
    async for body in channel.iter_body():
        cnt += 1
        print(cnt, body)
        if cnt > 10:
            break
        await channel.write(f"pong! {cnt}")


def match_demo_request(request: Request) -> limit.RULE_FUNC_RETURN_TYPE:
    if request.msg_type == constant.MT_CHANNEL and request.header.get("channel_life_cycle", "") != constant.DECLARE:
        return None, True
    if request.func_name in ("demo", "echo_body"):
        return request.func_name, False
    else:
        return None, False


def match_ip_request(request: Request) -> limit.RULE_FUNC_RETURN_TYPE:
    if request.msg_type == constant.MT_CHANNEL and request.header.get("channel_life_cycle", "") != constant.DECLARE:
        return None, True
    key: str = "127.0.0.1"
    if request.context.conn.peer_tuple[0] == "127.0.0.1":
        return key, False
    else:
        return None, False


def run_server() -> None:

    loop = asyncio.new_event_loop()
    redis: StrictRedis = StrictRedis.from_url("redis://localhost")
    rpc_server = Server()
    rpc_server.register(demo)
    rpc_server.register(demo1)
    rpc_server.register(echo_body)
    limit_processor = limit.LimitProcessor(
        limit.backend.RedisTokenBucketBackend(redis),
        [
            (match_demo_request, limit.Rule(second=1, gen_token=1, init_token=1, max_token=10, block_time=2)),
            (match_ip_request, limit.Rule(second=1, gen_token=1, init_token=1, max_token=10, block_time=2)),
        ],
    )
    rpc_server.load_processor([limit_processor])
    loop.run_until_complete(rpc_server.run_forever())


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        format="[%(asctime)s %(levelname)s] %(message)s", datefmt="%y-%m-%d %H:%M:%S", level=logging.INFO
    )
    run_server()
