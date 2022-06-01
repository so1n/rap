import asyncio
import inspect

import pytest

from rap.client.utils import raise_rap_error
from rap.common.cache import Cache
from rap.common.event import Event
from rap.common.exceptions import RPCError
from rap.common.state import State
from rap.common.utils import check_func_type

pytestmark = pytest.mark.asyncio


async def demo(a: int, b: int, c: int = 0) -> int:
    return a + b + c


class TestUtil:
    def test_state_class(self) -> None:
        state: State = State()
        state.demo = "123"
        assert 1 == len(state)

        del state.demo  # type: ignore

        with pytest.raises(AttributeError) as e:
            state.demo  # type: ignore

        exec_msg: str = e.value.args[0]
        assert exec_msg.endswith("object has no attribute 'demo'")

    def test_event_to_tuple(self) -> None:
        event: Event = Event(event_info="info", event_name="name")
        assert ("name", "info") == event.to_tuple()

    def test_raise_customer_exc(self) -> None:
        with pytest.raises(RPCError) as e:
            raise_rap_error("customer_exc", "customer_info")
        exec_msg: str = e.value.args[0]
        assert exec_msg == "customer_info"

    def test_check_func_type(self) -> None:
        def _demo(a: int, b: str, c: str = "") -> int:
            return 0

        with pytest.raises(TypeError):
            check_func_type(inspect.signature(_demo), (1, 2), {})

        with pytest.raises(TypeError):
            check_func_type(inspect.signature(_demo), (1, "2"), {"c": 3})

    async def test_cache(self) -> None:
        cache: Cache = Cache(1.5)
        cache.add("test1", 0.1)
        cache.add("test2", 1)
        assert "test1" in cache
        await asyncio.sleep(0.5)
        assert "test1" not in cache
        assert "test2" in cache
        await asyncio.sleep(1)
        assert "test2" not in cache
