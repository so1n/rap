import asyncio

import pytest

from rap.common.context import Context, WithContext

pytestmark = pytest.mark.asyncio


class TestContext:
    def test_context(self) -> None:
        class Demo(WithContext):
            bar: str

        context: Context = Context()

        with Demo() as d:
            d.bar = "abc"
            assert d.bar == "abc"
            assert context.bar == "abc"
        assert not d.bar
        assert not context.bar

    async def test_context_in_asyncio(self) -> None:
        class Demo(WithContext):
            bar: str

        context: Context = Context()

        with Demo() as d:

            async def demo() -> None:
                assert d.bar == "abc"
                assert context.bar == "abc"

            d.bar = "abc"
            assert context.bar == "abc"
            await asyncio.gather(demo(), demo())
