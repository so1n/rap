import asyncio
import pytest
from typing import Any

from aredis import StrictRedis
from rap.client import Client
from rap.server import Server
from rap.server.middleware.conn.block import IpBlockMiddleware
from rap.server.middleware.conn.conn_limit import ConnLimitMiddleware, IpMaxConnMiddleware
from rap.server.middleware.msg.access import AccessMsgMiddleware 


pytestmark = pytest.mark.asyncio


async def mock_func(*args: Any):
    await asyncio.sleep(0)


class TestMiddleware:
    async def test_access(self, rap_server: Server) -> None:
        rap_server.load_middleware([AccessMsgMiddleware()])
        client: Client = Client()
        await client.connect()
        assert 3 == await client.raw_call("async_sum", 1, 2)

    async def test_conn_limit(self, rap_server: Server) -> None:
        rap_server.load_middleware([ConnLimitMiddleware(max_conn=0)])
        client: Client = Client()
        client.transport._listen = mock_func
        await client.connect()

        for conn_model in client.transport._conn_dict.values():
            with pytest.raises(ConnectionError):
                await client.transport._read_from_conn(conn_model.conn)

    async def test_ip_limit(self, rap_server: Server) -> None:
        redis: StrictRedis = StrictRedis.from_url("redis://localhost")
        rap_server.load_middleware([IpMaxConnMiddleware(redis, ip_max_conn=0)])
        client: Client = Client()
        client.transport._listen = mock_func
        await client.connect()

        for conn_model in client.transport._conn_dict.values():
            with pytest.raises(ConnectionError):
                await client.transport._read_from_conn(conn_model.conn)

    async def test_allow_ip_access(self, rap_server: Server) -> None:
        redis: StrictRedis = StrictRedis.from_url("redis://localhost")
        middleware: IpBlockMiddleware = IpBlockMiddleware(redis, allow_ip_list=["127.0.0.2"])
        rap_server.load_middleware([middleware])
        await middleware.start_event_handle()
        client: Client = Client()
        client.transport._listen = mock_func
        await client.connect()

        for conn_model in client.transport._conn_dict.values():
            with pytest.raises(ConnectionError):
                await client.transport._read_from_conn(conn_model.conn)

    async def test_black_ip_access(self, rap_server: Server) -> None:
        redis: StrictRedis = StrictRedis.from_url("redis://localhost")
        rap_server.load_middleware([IpBlockMiddleware(redis, block_ip_list=["127.0.0.1"])])
        client: Client = Client()
        client.transport._listen = mock_func
        await client.connect()

        for conn_model in client.transport._conn_dict.values():
            with pytest.raises(ConnectionError):
                await client.transport._read_from_conn(conn_model.conn)
