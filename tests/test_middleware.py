import asyncio
from copy import deepcopy
from typing import Any

import pytest
from aredis import StrictRedis  # type: ignore

from rap.client import Client
from rap.server import Server
from rap.server.plugin.middleware.conn.block import IpBlockMiddleware
from rap.server.plugin.middleware.conn.limit import ConnLimitMiddleware, IpMaxConnMiddleware

pytestmark = pytest.mark.asyncio


async def mock_func(self: Any) -> None:
    await asyncio.sleep(0)


async def clean_cache_ip_before_test(middleware: IpBlockMiddleware) -> None:
    for ip in await middleware._get_allow_ip():
        await middleware._remove_allow_ip(ip)
    for ip in await middleware._get_block_ip():
        await middleware._remove_block_ip(ip)


class TestConnLimitMiddleware:
    async def test_conn_limit(self, rap_server: Server) -> None:
        rap_server.load_middleware([ConnLimitMiddleware(max_conn=0)])

        client: Client = Client("test")
        client.add_conn("localhost", 9000)
        setattr(client.transport, "listen", mock_func)
        await client.start()

        for conn_model in client.transport._conn_dict.copy().values():
            with pytest.raises(ConnectionError):
                await client.transport._read_from_conn(conn_model.conn)

    async def test_conn_limit_allow(self, rap_server: Server) -> None:
        rap_server.load_middleware([ConnLimitMiddleware(max_conn=1)])
        client: Client = Client("test")
        client.add_conn("localhost", 9000)
        await client.start()
        assert 3 == await client.raw_call("async_sum", [1, 2])

    async def test_conn_limit_method(self, rap_server: Server, rap_client: Client) -> None:
        middleware: ConnLimitMiddleware = ConnLimitMiddleware(max_conn=0)
        rap_server.load_middleware([middleware])
        middleware.start_event_handle()
        await rap_client.raw_call("modify_max_conn", [10], group=middleware.__class__.__name__)
        assert middleware._max_conn == 10
        await rap_client.raw_call("modify_release_timestamp", [1_600_000_000], group=middleware.__class__.__name__)
        assert middleware._release_timestamp == 1_600_000_000
        assert {
            "conn_count": middleware._conn_count,
            "max_conn": 10,
            "release_timestamp": 1_600_000_000,
        } == await rap_client.raw_call("get_conn_limit_info", group=middleware.__class__.__name__)


class TestIpMaxConnMiddleware:
    async def test_ip_max_conn_method(self, rap_server: Server, rap_client: Client) -> None:
        redis: StrictRedis = StrictRedis.from_url("redis://localhost")
        middleware: IpMaxConnMiddleware = IpMaxConnMiddleware(redis, ip_max_conn=0)
        rap_server.load_middleware([middleware])
        middleware.start_event_handle()
        await rap_client.raw_call("modify_max_ip_max_conn", [10], group=middleware.__class__.__name__)
        assert middleware._ip_max_conn == 10
        await rap_client.raw_call("modify_ip_max_timeout", [10], group=middleware.__class__.__name__)
        assert middleware._timeout == 10
        assert {"ip_max_conn": 10, "timeout": 10} == await rap_client.raw_call(
            "get_info", group=middleware.__class__.__name__
        )

    async def test_ip_max_conn(self, rap_server: Server) -> None:
        redis: StrictRedis = StrictRedis.from_url("redis://localhost")
        middleware: IpMaxConnMiddleware = IpMaxConnMiddleware(redis, ip_max_conn=0)
        rap_server.load_middleware([middleware])
        client: Client = Client("test")
        client.add_conn("localhost", 9000)
        setattr(client.transport, "listen", mock_func)
        await client.start()

        for conn_model in client.transport._conn_dict.copy().values():
            with pytest.raises(ConnectionError):
                await client.transport._read_from_conn(conn_model.conn)

    async def test_ip_max_conn_allow(self, rap_server: Server) -> None:
        redis: StrictRedis = StrictRedis.from_url("redis://localhost")
        middleware: IpMaxConnMiddleware = IpMaxConnMiddleware(redis, ip_max_conn=1)
        rap_server.load_middleware([middleware])
        client: Client = Client("test")
        client.add_conn("localhost", 9000)
        await client.start()
        assert 3 == await client.raw_call("async_sum", [1, 2])


class TestIpBlockMiddleware:
    async def test_ip_block_method(self, rap_server: Server) -> None:
        redis: StrictRedis = StrictRedis.from_url("redis://localhost")
        middleware: IpBlockMiddleware = IpBlockMiddleware(redis)
        await clean_cache_ip_before_test(middleware)

        rap_server.load_middleware([middleware])
        await middleware.start_event_handle()

        client: Client = Client("test")
        client.add_conn("localhost", 9000)
        await client.start()
        await client.raw_call("add_block_ip", ["127.0.0.1"], group=middleware.__class__.__name__)
        await client.raw_call("add_allow_ip", ["127.0.0.1"], group=middleware.__class__.__name__)
        await client.raw_call("remove_block_ip", ["127.0.0.1"], group=middleware.__class__.__name__)
        await client.raw_call("remove_allow_ip", ["127.0.0.1"], group=middleware.__class__.__name__)
        await client.stop()

    async def test_ip_block_ip_in_access_list(self, rap_server: Server) -> None:
        redis: StrictRedis = StrictRedis.from_url("redis://localhost")
        middleware: IpBlockMiddleware = IpBlockMiddleware(
            redis, allow_ip_list=["localhost", "::1", "127.0.0.1", "192.168.0.0/31"]
        )
        await clean_cache_ip_before_test(middleware)

        rap_server.load_middleware([middleware])
        await middleware.start_event_handle()
        client: Client = Client("test")
        client.add_conn("localhost", 9000)
        await client.start()
        assert 3 == await client.raw_call("async_sum", [1, 2])
        await client.stop()

    async def test_ip_block_by_allow_ip_access(self, rap_server: Server) -> None:
        redis: StrictRedis = StrictRedis.from_url("redis://localhost")
        middleware: IpBlockMiddleware = IpBlockMiddleware(redis, allow_ip_list=["127.0.0.2"])
        await clean_cache_ip_before_test(middleware)

        rap_server.load_middleware([middleware])
        await middleware.start_event_handle()
        client: Client = Client("test")
        client.add_conn("localhost", 9000)
        setattr(client.transport, "listen", mock_func)
        await client.start()

        for conn_model in client.transport._conn_dict.copy().values():
            with pytest.raises(ConnectionError):
                await client.transport._read_from_conn(conn_model.conn)

    async def test_ip_block_ip_not_in_block_list(self, rap_server: Server) -> None:
        redis: StrictRedis = StrictRedis.from_url("redis://localhost")
        middleware: IpBlockMiddleware = IpBlockMiddleware(redis, block_ip_list=["127.0.0.2"])
        await clean_cache_ip_before_test(middleware)

        rap_server.load_middleware([middleware])
        await middleware.start_event_handle()
        client: Client = Client("test")
        client.add_conn("localhost", 9000)
        await client.start()
        assert 3 == await client.raw_call("async_sum", [1, 2])
        await client.stop()

    async def test_ip_block_by_black_ip_access(self, rap_server: Server) -> None:
        redis: StrictRedis = StrictRedis.from_url("redis://localhost")
        middleware: IpBlockMiddleware = IpBlockMiddleware(redis, block_ip_list=["localhost", "::1", "127.0.0.1"])
        await clean_cache_ip_before_test(middleware)

        rap_server.load_middleware([middleware])
        await middleware.start_event_handle()

        client: Client = Client("test")
        client.add_conn("localhost", 9000)
        setattr(client.transport, "listen", mock_func)
        await client.start()

        for conn_model in client.transport._conn_dict.copy().values():
            with pytest.raises(ConnectionError):
                await client.transport._read_from_conn(conn_model.conn)
