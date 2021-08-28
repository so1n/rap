from typing import AsyncIterator

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
            registry.register(pytest)  # type: ignore
        exec_msg = e.value.args[0]
        assert exec_msg == "func must be func or method"

    async def test_not_return_annotation(self) -> None:
        def demo():  # type: ignore
            pass

        with pytest.raises(RegisteredError) as e:
            registry.register(demo)

        exec_msg = e.value.args[0]
        assert exec_msg == f"{demo.__name__} must use TypeHints"

        def demo1() -> RegistryManager:
            pass

        with pytest.raises(RegisteredError) as e:
            registry.register(demo1)

        exec_msg = e.value.args[0]
        assert exec_msg == f"{demo1.__name__} return type:{RegistryManager} is not json type"

    async def test_param_type(self) -> None:
        def demo(a) -> None:  # type: ignore
            pass

        with pytest.raises(RegisteredError) as e:
            registry.register(demo)

        exec_msg: str = e.value.args[0]
        assert exec_msg == f"{demo.__name__} param:a must use TypeHints"

        def demo1(a: RegistryManager) -> None:
            pass

        with pytest.raises(RegisteredError) as e:
            registry.register(demo1)

        exec_msg = e.value.args[0]
        assert exec_msg == f"{demo1.__name__} param:a type:{RegistryManager} is not json type"

    async def test_repeat_register(self) -> None:
        def demo() -> None:
            pass

        registry.register(demo)

        with pytest.raises(RegisteredError) as e:
            registry.register(demo)
        exec_msg: str = e.value.args[0]
        assert exec_msg == "`normal:default:demo` Already register"

    async def test_reload_module(self, rap_server: Server, rap_client: Client) -> None:
        @rap_client.register()
        async def reload_sum_num(a: int, b: int) -> int:
            pass

        def _reload_sum_num(a: int, b: int) -> int:
            return a + b

        rap_server.register(_reload_sum_num, "reload_sum_num")

        assert 3 == await reload_sum_num(1, 2)
        await rap_client.raw_invoke(
            "reload",
            ["tests.test_register", "new_reload_sum", "reload_sum_num"],
            group="registry",
        )
        assert 4 == await reload_sum_num(1, 2)

        with pytest.raises(RegisteredError) as e:
            await rap_client.raw_invoke(
                "reload",
                ["tests.test_register", "new_reload_sum", "load", "registry"],
                group="registry",
            )
        exec_msg: str = e.value.args[0]
        assert exec_msg.endswith("private func can not reload")

        with pytest.raises(RegisteredError) as e:
            await rap_client.raw_invoke("reload", ["tests.test_register", "new_reload_sum", "load"], group="registry")
        exec_msg = e.value.args[0]
        assert "`normal:default:load` not exists" in exec_msg

    async def test_load_error_fun(self, rap_server: Server, rap_client: Client) -> None:
        with pytest.raises(RegisteredError) as e:
            await rap_client.raw_invoke("load", ["tests.test_register", "fail_reload_demo"], group="registry")
        exec_msg: str = e.value.args[0]
        assert exec_msg.endswith("is not a callable object")

    async def test_load_fun(self, rap_server: Server, rap_client: Client) -> None:
        await rap_client.raw_invoke("load", ["tests.test_register", "new_reload_sum"], group="registry")
        assert 4 == await rap_client.raw_invoke("new_reload_sum", [1, 2])

        with pytest.raises(RegisteredError) as e:
            rap_server.registry._load("tests.test_register", "new_reload_sum")
        exec_msg: str = e.value.args[0]
        assert "`normal:default:new_reload_sum` already exists" in exec_msg

    async def test_register_func_error(self, rap_server: Server, rap_client: Client) -> None:
        def test_func() -> None:
            pass

        with pytest.raises(TypeError):
            rap_client.register()(test_func)

    async def test_register_func_check_type_error_in_runtime(self, rap_server: Server, rap_client: Client) -> None:
        @rap_client.register()
        async def demo1(a: int, b: int) -> str:
            return a + b  # type: ignore

        rap_server.register(demo1)
        with pytest.raises(TypeError):
            await demo1(1, "1")  # type: ignore

        with pytest.raises(RuntimeError):
            await demo1(1, 1)

    async def test_register_gen_func_check_type_error_in_runtime(self, rap_server: Server, rap_client: Client) -> None:
        @rap_client.register()
        async def demo1(a: int) -> AsyncIterator[str]:
            yield a  # type: ignore

        async def _demo1(a: int) -> AsyncIterator[int]:
            for i in range(a):
                yield i

        rap_server.register(_demo1, name="demo1")
        with pytest.raises(TypeError):
            async for i in demo1("1"):  # type: ignore
                print(i)

        with pytest.raises(RuntimeError):
            async for i in demo1(10):
                print(i)
