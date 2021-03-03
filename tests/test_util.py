import pytest
from rap.common.utlis import Event, State, gen_new_param_coro


pytestmark = pytest.mark.asyncio


async def demo(a: int, b: int) -> int:
    return a + b


class TestUtil:
    def test_state_class(self) -> None:
        state: State = State()
        state.demo = '123'
        assert 1 == len(state)

        del state.demo

        with pytest.raises(AttributeError) as e:
            state.demo

        exec_msg: str = e.value.args[0]
        assert exec_msg.endswith("object has no attribute 'demo'")

    def test_event_to_tuple(self) -> None:
        event: Event = Event('name', 'info')
        assert ('name', 'info') == event.to_tuple()

    async def test_gen_new_param_coro(self) -> None:

        value1: int = await demo(1, 3)
        new_coro = demo(1, 5)
        value2: int = await gen_new_param_coro(new_coro, {"b": 3})
        assert 6 == await new_coro
        assert value1 == value2
