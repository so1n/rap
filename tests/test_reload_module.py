import pytest

from rap.client import Client
from rap.server import Server

pytestmark = pytest.mark.asyncio


def new_reload_sum(a: int, b: int) -> int:
    return a + b + a


class TestReloadModule:
    async def test_reload_module(self, rap_server: Server, rap_client: Client) -> None:
        @rap_client.register()
        async def reload_sum_num(a: int , b: int) -> int:
            pass

        def _reload_sum_num(a: int, b: int) -> int:
            return a + b

        rap_server.register(_reload_sum_num, "reload_sum_num")

        assert 3 == await reload_sum_num(1, 2)
        await rap_client.raw_call('reload', 'tests.test_reload_module', 'new_reload_sum', "reload_sum_num", "default", None, group='registry')
        assert 4 == await reload_sum_num(1, 2)
