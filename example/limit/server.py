import asyncio

from rap.server import Server
from rap.server.model import RequestModel
from rap.server.processor import limit


async def demo(a: int, b: int) -> int:
    return a + b


async def demo1(a: int, b: int) -> int:
    return a + b


def match_demo_request(request: RequestModel) -> limit.RULE_FUNC_RETURN_TYPE:
    if request.func_name == 'demo':
        return request.func_name
    else:
        return None


def match_ip_request(request: RequestModel) -> limit.RULE_FUNC_RETURN_TYPE:
    key: str = "127.0.0.1"
    if request.header["host"][0] == key:
        return key
    else:
        return None


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        format="[%(asctime)s %(levelname)s] %(message)s", datefmt="%y-%m-%d %H:%M:%S", level=logging.INFO
    )

    loop = asyncio.new_event_loop()
    rpc_server = Server()
    rpc_server.register(demo)
    rpc_server.register(demo1)
    limit_processor = limit.LimitProcessor(
        limit.backend.RedisTokenBucketBackend(rpc_server.redis),
        [
            (match_demo_request, limit.Rule(second=5, gen_token=1, init_token=1, max_token=10, block_time=10)),
            (match_ip_request, limit.Rule(second=5, gen_token=1, init_token=1, max_token=10, block_time=10))
        ]
    )
    rpc_server.load_processor([limit_processor])
    loop.run_until_complete(rpc_server.create_server())

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        loop.run_until_complete(rpc_server.await_closed())
