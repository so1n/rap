import asyncio

import pytest

from rap.client.processor.crypto import AutoExpireSet
from rap.client.utils import raise_rap_error
from rap.common.exceptions import RPCError
from rap.common.utils import Event, State, check_func_type, gen_new_param_coro

pytestmark = pytest.mark.asyncio


async def demo(a: int, b: int, c: int = 0) -> int:
    return a + b + c


class TestUtil:
    def test_state_class(self) -> None:
        state: State = State()
        state.demo = "123"
        assert 1 == len(state)

        del state.demo

        with pytest.raises(AttributeError) as e:
            state.demo

        exec_msg: str = e.value.args[0]
        assert exec_msg.endswith("object has no attribute 'demo'")

    def test_event_to_tuple(self) -> None:
        event: Event = Event("name", "info")
        assert ("name", "info") == event.to_tuple()

    async def test_gen_new_param_coro(self) -> None:

        value1: int = await demo(1, 3)
        new_coro = demo(1, 5)

        with pytest.raises(TypeError):
            gen_new_param_coro(1, {"d": 3})

        with pytest.raises(KeyError):
            await gen_new_param_coro(new_coro, {"d": 3})

        value2: int = await gen_new_param_coro(new_coro, {"b": 3})
        assert 6 == await new_coro
        assert value1 == value2

    def test_raise_customer_exc(self) -> None:
        with pytest.raises(RPCError) as e:
            raise_rap_error("customer_exc", "customer_info")
        exec_msg: str = e.value.args[0]
        assert exec_msg == "customer_info"

    def test_check_func_type(self) -> None:
        def _demo(a: int, b: str, c: str = "") -> int:
            pass

        with pytest.raises(TypeError):
            check_func_type(_demo, (1, 2), {})

        with pytest.raises(TypeError):
            check_func_type(_demo, (1, "2"), {"c": 3})

    async def test_auto_expire_set(self) -> None:
        auto_expire_set: AutoExpireSet = AutoExpireSet(1.5)
        auto_expire_set.add("test1", 0.1)
        auto_expire_set.add("test2", 1)
        assert "test1" in auto_expire_set
        await asyncio.sleep(0.5)
        assert "test1" not in auto_expire_set
        assert "test2" in auto_expire_set
        await asyncio.sleep(1)
        assert "test2" not in auto_expire_set
