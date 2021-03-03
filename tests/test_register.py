from typing import Dict, List, Union

import pytest

from rap.client import Client
from rap.common.exceptions import RegisteredError
from rap.server import Server
from rap.server.registry import RegistryManager

pytestmark = pytest.mark.asyncio
registry: RegistryManager = RegistryManager()
fail_reload_demo: int = 10


def new_reload_sum(a: int, b: int) -> int:
    return a + b + a


class TestRegister:
    async def test_register_error_value(self) -> None:
        with pytest.raises(RegisteredError) as e:
            registry.register(pytest)
        exec_msg = e.value.args[0]
        assert exec_msg == 'func must be func or method'

    async def test_not_return_annotation(self) -> None:
        def demo(): pass

        with pytest.raises(RegisteredError) as e:
            registry.register(demo)

        exec_msg = e.value.args[0]
        assert exec_msg == f"{demo.__name__} must use TypeHints"

        def demo1() -> RegistryManager: pass

        with pytest.raises(RegisteredError) as e:
            registry.register(demo1)

        exec_msg = e.value.args[0]
        assert exec_msg == f"{demo1.__name__} return type:{RegistryManager} is not json type"

    async def test_param_type(self) -> None:
        def demo(a) -> None: pass

        with pytest.raises(RegisteredError) as e:
            registry.register(demo)

        exec_msg = e.value.args[0]
        assert exec_msg == f"{demo.__name__} param:a must use TypeHints"

        def demo1(a: RegistryManager) -> None: pass

        with pytest.raises(RegisteredError) as e:
            registry.register(demo1)

        exec_msg: str = e.value.args[0]
        assert exec_msg == f"{demo1.__name__} param:a type:{RegistryManager} is not json type"

    async def test_repeat_register(self) -> None:
        def demo() -> None: pass

        registry.register(demo)

        with pytest.raises(RegisteredError) as e:
            registry.register(demo)
        exec_msg: str = e.value.args[0]
        assert exec_msg == "Name: demo has already been used"

    async def test_reload_module(self, rap_server: Server, rap_client: Client) -> None:

        @rap_client.register()
        async def reload_sum_num(a: int, b: int) -> int:
            pass

        def _reload_sum_num(a: int, b: int) -> int:
            return a + b

        rap_server.register(_reload_sum_num, "reload_sum_num")

        assert 3 == await reload_sum_num(1, 2)
        await rap_client.raw_call('reload', 'tests.test_register', 'new_reload_sum', "reload_sum_num", "default",
                                  None, group='registry')
        assert 4 == await reload_sum_num(1, 2)

        with pytest.raises(RegisteredError) as e:
            await rap_client.raw_call(
                'reload', 'tests.test_register', 'new_reload_sum', "load", "registry", None, group='registry'
            )
        exec_msg: str = e.value.args[0]
        assert exec_msg.endswith("private func can not reload")

        with pytest.raises(RegisteredError) as e:
            await rap_client.raw_call(
                'reload', 'tests.test_register', 'new_reload_sum', "load", "default", None, group='registry'
            )
        exec_msg = e.value.args[0]
        assert "not in group" in exec_msg

    async def test_load_error_fun(self, rap_server: Server, rap_client: Client) -> None:
        with pytest.raises(RegisteredError) as e:
            await rap_client.raw_call(
                "load", "tests.test_register", "fail_reload_demo", None, "default", False, None, group='registry'
            )
        exec_msg: str = e.value.args[0]
        assert exec_msg.endswith("is not a callable object")

    async def test_load_fun(self, rap_server: Server, rap_client: Client) -> None:
        await rap_client.raw_call(
            "load", "tests.test_register", "new_reload_sum", None, "default", False, None, group='registry'
        )
        assert 4 == await rap_client.raw_call("new_reload_sum", 1, 2)

        with pytest.raises(RegisteredError) as e:
            rap_server.registry._load("tests.test_register", "new_reload_sum")
        exec_msg: str = e.value.args[0]
        assert "already exists in group " in exec_msg


