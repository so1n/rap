import asyncio
from typing import Any

import pytest
from aredis import StrictRedis  # type: ignore

from rap.client import Client
from rap.common.conn import CloseConnException
from rap.server import Server
from rap.server.plugin.middleware.conn.ip_filter import IpFilterMiddleware
from rap.server.plugin.middleware.conn.limit import ConnLimitMiddleware, IpMaxConnMiddleware

pytestmark = pytest.mark.asyncio


async def mock_func(self: Any) -> None:
    await asyncio.sleep(0)


async def clean_cache_ip_before_test(middleware: IpFilterMiddleware) -> None:
    for ip in await middleware._get_allow_ip():
        await middleware._remove_allow_ip(ip)
    for ip in await middleware._get_block_ip():
        await middleware._remove_block_ip(ip)


class TestConnLimitMiddleware:
    async def test_conn_limit(self, rap_server: Server) -> None:
        rap_server.load_middleware([ConnLimitMiddleware(max_conn=1)])

        client_1: Client = Client("test")
        client_2: Client = Client("test")
        await client_1.start()
        with pytest.raises(CloseConnException) as e:
            await client_2.start()

        exec_msg: str = e.value.args[0]
        assert exec_msg == "Currently exceeding the maximum number of connections limit"
        await client_1.stop()
        await client_2.stop()

    async def test_conn_limit_method(self, rap_server: Server, rap_client: Client) -> None:
        middleware: ConnLimitMiddleware = ConnLimitMiddleware(max_conn=0)
        rap_server.load_middleware([middleware])
        middleware.start_event_handle(rap_server)
        await rap_client.invoke_by_name("modify_max_conn", [10], group=middleware.__class__.__name__)
        assert middleware._max_conn == 10
        await rap_client.invoke_by_name(
            "modify_release_timestamp", [1_600_000_000], group=middleware.__class__.__name__
        )
        assert middleware._release_timestamp == 1_600_000_000
        assert {
            "conn_count": middleware._conn_count,
            "max_conn": 10,
            "release_timestamp": 1_600_000_000,
        } == await rap_client.invoke_by_name("get_conn_limit_info", group=middleware.__class__.__name__)


class TestIpMaxConnMiddleware:
    async def test_ip_max_conn_method(self, rap_server: Server, rap_client: Client) -> None:
        redis: StrictRedis = StrictRedis.from_url("redis://localhost")
        middleware: IpMaxConnMiddleware = IpMaxConnMiddleware(redis, ip_max_conn=0)
        rap_server.load_middleware([middleware])
        middleware.start_event_handle(rap_server)
        await rap_client.invoke_by_name("modify_max_ip_max_conn", [10], group=middleware.__class__.__name__)
        assert middleware._ip_max_conn == 10
        await rap_client.invoke_by_name("modify_ip_max_timeout", [10], group=middleware.__class__.__name__)
        assert middleware._timeout == 10
        assert {"ip_max_conn": 10, "timeout": 10} == await rap_client.invoke_by_name(
            "get_info", group=middleware.__class__.__name__
        )

    async def test_ip_max_conn(self, rap_server: Server) -> None:
        redis: StrictRedis = StrictRedis.from_url("redis://localhost")
        middleware: IpMaxConnMiddleware = IpMaxConnMiddleware(redis, ip_max_conn=1)
        for key in await redis.keys(middleware._key + "*"):
            await redis.delete(key)
        rap_server.load_middleware([middleware])
        client_1: Client = Client("test")
        client_2: Client = Client("test")
        await client_1.start()
        with pytest.raises(CloseConnException):
            await client_2.start()
        await client_1.stop()
        await client_2.stop()


class TestIpBlockMiddleware:
    async def test_ip_block_method(self, rap_server: Server) -> None:
        redis: StrictRedis = StrictRedis.from_url("redis://localhost")
        middleware: IpFilterMiddleware = IpFilterMiddleware(redis)
        await clean_cache_ip_before_test(middleware)

        rap_server.load_middleware([middleware])
        await middleware.start_event_handle(rap_server)

        client: Client = Client("test")
        await client.start()
        await client.invoke_by_name("add_block_ip", ["127.0.0.1"], group=middleware.__class__.__name__)
        await client.invoke_by_name("add_allow_ip", ["127.0.0.1"], group=middleware.__class__.__name__)
        await client.invoke_by_name("remove_block_ip", ["127.0.0.1"], group=middleware.__class__.__name__)
        await client.invoke_by_name("remove_allow_ip", ["127.0.0.1"], group=middleware.__class__.__name__)
        await client.stop()

    async def test_ip_block_ip_in_access_list(self, rap_server: Server) -> None:
        redis: StrictRedis = StrictRedis.from_url("redis://localhost")
        middleware: IpFilterMiddleware = IpFilterMiddleware(
            redis, allow_ip_list=["localhost", "::1", "127.0.0.1", "192.168.0.0/31"]
        )
        await clean_cache_ip_before_test(middleware)

        rap_server.load_middleware([middleware])
        await middleware.start_event_handle(rap_server)
        # allow access
        client: Client = Client("test")
        await client.start()
        assert 3 == await client.invoke_by_name("async_sum", [1, 2])
        # block
        await client.invoke_by_name(
            "remove_allow_ip",
            [["localhost", "::1", "127.0.0.1", "192.168.0.0/31"]],
            group=middleware.__class__.__name__,
        )
        await client.invoke_by_name("add_allow_ip", ["1.1.1.1"], group=middleware.__class__.__name__)
        await client.stop()
        with pytest.raises(CloseConnException):
            await client.start()
        await client.stop()

    async def test_ip_block_ip_not_in_block_list(self, rap_server: Server) -> None:
        redis: StrictRedis = StrictRedis.from_url("redis://localhost")
        middleware: IpFilterMiddleware = IpFilterMiddleware(redis, block_ip_list=["127.0.0.2"])
        await clean_cache_ip_before_test(middleware)

        rap_server.load_middleware([middleware])
        await middleware.start_event_handle(rap_server)
        client: Client = Client("test")
        await client.start()
        assert 3 == await client.invoke_by_name("async_sum", [1, 2])
        await client.invoke_by_name(
            "add_block_ip", [["localhost", "::1", "127.0.0.1", "192.168.0.0/31"]], group=middleware.__class__.__name__
        )
        await client.stop()
        with pytest.raises(CloseConnException):
            await client.start()

    async def test_ip_block_by_black_ip_access(self, rap_server: Server) -> None:
        redis: StrictRedis = StrictRedis.from_url("redis://localhost")
        middleware: IpFilterMiddleware = IpFilterMiddleware(redis, block_ip_list=["localhost", "::1", "127.0.0.1"])
        await clean_cache_ip_before_test(middleware)

        rap_server.load_middleware([middleware])
        await middleware.start_event_handle(rap_server)

        client: Client = Client("test")
        with pytest.raises(CloseConnException):
            await client.start()
        await client.stop()
