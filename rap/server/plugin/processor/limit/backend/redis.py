import asyncio
import time
from abc import ABC
from typing import Any, Awaitable, Callable, Coroutine, List, Optional, Union

from aredis import StrictRedis, StrictRedisCluster  # type: ignore

from rap.server.plugin.processor.limit.backend import BaseLimitBackend
from rap.server.plugin.processor.limit.rule import Rule


class BaseRedisBackend(BaseLimitBackend, ABC):
    def __init__(self, redis: Union[StrictRedis, StrictRedisCluster]):
        self._redis: "Union[StrictRedis, StrictRedisCluster]" = redis

    async def _block_time_handle(self, key: str, rule: Rule, func: Callable[..., Awaitable[bool]]) -> bool:
        block_time_key: str = f"{key}:block_time"
        bucket_block_time: Optional[int] = rule.block_time

        if bucket_block_time is not None and await self._redis.exists(block_time_key):
            return False

        can_requests: bool = await func()
        if not can_requests and bucket_block_time is not None:
            await self._redis.set(block_time_key, bucket_block_time, ex=bucket_block_time)

        return can_requests


class RedisFixedWindowBackend(BaseRedisBackend):
    def can_requests(self, key: str, rule: Rule, token_num: int = 1) -> Union[bool, Coroutine[Any, Any, bool]]:
        async def _can_requests() -> bool:
            """
            In the current time(rule.get_second()) window,
             whether the existing value(access_num) exceeds the maximum value(rule.gen_token_num)
            """
            access_num: int = await self._redis.incr(key)
            if access_num == 1:
                await self._redis.expire(key, int(rule.total_second))

            can_requests: bool = not (access_num > rule.gen_token)
            return can_requests

        return self._block_time_handle(key, rule, _can_requests)

    def expected_time(self, key: str, rule: Rule) -> Union[float, Coroutine[Any, Any, float]]:
        async def _expected_time() -> float:
            block_time_key: str = key + ":block_time"
            block_time = await self._redis.get(block_time_key)
            if block_time:
                return await self._redis.ttl(block_time_key)

            token_num: Optional[str] = await self._redis.get(key)
            if token_num is None:
                return 0
            else:
                token_num_int: int = int(token_num)

                if token_num_int < rule.gen_token:
                    return 0
                return await self._redis.ttl(key)

        return _expected_time()


class BaseRedisCellBackend(BaseRedisBackend):
    """
    use redis-cell module
    learn more:https://github.com/brandur/redis-cell

    input: CL.THROTTLE user123 15 30 60 1
        # param  |  desc
        # user123 key
        # 15 maxburst
        # 30 token
        # 60 seconds
        # 1 apply 1token
    output:
        1) (integer) 0        # is allowed
        2) (integer) 16       # total bucket num
        3) (integer) 15       # the remaining limit of the key.
        4) (integer) -1       # the number of seconds until the user should retry,
                              #   and always -1 if the action was allowed.
        5) (integer) 2        # The number of seconds until the limit will reset to its maximum capacity
    """

    async def _call_cell(self, key: str, rule: Rule, token_num: int = 1) -> List[int]:
        result: List[int] = await self._redis.execute_command(
            "CL.THROTTLE", key, rule.max_token - 1, rule.gen_token, int(rule.total_second), token_num
        )
        return result

    def expected_time(self, key: str, rule: Rule) -> Union[float, Coroutine[Any, Any, float]]:
        async def _expected_time() -> float:
            block_time_key: str = key + ":block_time"
            block_time = await self._redis.get(block_time_key)
            if block_time:
                return await self._redis.ttl(block_time_key)

            result: List[int] = await self._call_cell(key, rule, 0)
            return float(max(result[3], 0))

        return _expected_time()


class RedisCellBackend(BaseRedisCellBackend):
    def can_requests(self, key: str, rule: Rule, token_num: int = 1) -> Union[bool, Coroutine[Any, Any, bool]]:
        async def _can_requests() -> bool:
            result: List[int] = await self._call_cell(key, rule, token_num)
            can_requests: bool = result[0] == 0
            if can_requests and result[4]:
                await asyncio.sleep(4)
            return can_requests

        return self._block_time_handle(key, rule, _can_requests)


class RedisCellLikeTokenBucketBackend(BaseRedisCellBackend):
    def can_requests(self, key: str, rule: Rule, token_num: int = 1) -> Union[bool, Coroutine[Any, Any, bool]]:
        async def _can_requests() -> bool:
            result: List[int] = await self._call_cell(key, rule, token_num)
            can_requests: bool = result[0] == 0
            return can_requests

        return self._block_time_handle(key, rule, _can_requests)


class RedisTokenBucketBackend(BaseRedisBackend):
    _lua_script = """
local key = KEYS[1]
local current_time = tonumber(ARGV[1])
local interval_per_token = tonumber(ARGV[2])
local max_token = tonumber(ARGV[3])
local init_token = tonumber(ARGV[4])
local tokens
local bucket = redis.call("hmget", key, "last_time", "last_token")
local last_time= bucket[1]
local last_token = bucket[2]
if last_time == false or last_token == false then
    tokens = init_token
    redis.call('hset', key, 'last_time', current_time)
else
    local this_interval = current_time - tonumber(last_time)
    if this_interval > 1 then
        local tokens_to_add = math.floor(this_interval * interval_per_token)
        tokens = math.min(last_token + tokens_to_add, max_token)
        redis.call('hset', key, 'last_time', current_time)
    else
        tokens = tonumber(last_token)
    end
end
if tokens < 1 then
    redis.call('hset', key, 'last_token', tokens)
    return -1
else
    tokens = tokens - 1
    redis.call('hset', key, 'last_token', tokens)
    return tokens
end
    """

    def can_requests(self, key: str, rule: Rule, token_num: int = 1) -> Union[bool, Coroutine[Any, Any, bool]]:
        async def _can_requests() -> bool:
            now_token: int = await self._redis.eval(
                self._lua_script, 1, key, time.time(), rule.rate, rule.max_token, rule.init_token
            )
            await self._redis.expire(key, int(rule.total_second) * 2)
            return now_token >= 0

        return self._block_time_handle(key, rule, _can_requests)

    def expected_time(self, key: str, rule: Rule) -> Union[float, Coroutine[Any, Any, float]]:
        async def _expected_time() -> float:
            block_time_key: str = key + ":block_time"
            block_time = await self._redis.get(block_time_key)
            if block_time:
                return await self._redis.ttl(block_time_key)
            last_time = await self._redis.hget(key, "last_time")
            if last_time is None:
                return 0
            diff_time = last_time - time.time() * 1000
            if diff_time > 0:
                return diff_time
            return 0

        return _expected_time()
