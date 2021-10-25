import asyncio
import time
from typing import Coroutine, Optional

import pytest

from rap.common import asyncio_helper

pytestmark = pytest.mark.asyncio


async def demo(a: int, b: int) -> int:
    return a + b


class TestAsyncioHelper:
    async def test_current_task(self) -> None:
        task: Optional[asyncio.Task] = asyncio_helper.current_task()
        assert task
        assert "test_current_task" in str(task)

    async def test_get_event_loop(self) -> None:
        loop: asyncio.AbstractEventLoop = asyncio_helper.get_event_loop()
        assert loop.is_running()

    async def test_done_future(self) -> None:
        assert asyncio_helper.done_future().done()

    async def test_can_cancel_sleep(self) -> None:
        exc: Optional[Exception] = None
        try:
            future: asyncio.Future = asyncio.ensure_future(asyncio_helper.can_cancel_sleep(10))
            await asyncio.sleep(0)
            future.cancel()
            await asyncio.sleep(0)
            future.result()
        except Exception as e:
            exc = e
        assert isinstance(exc, asyncio.CancelledError)

    async def test_gen_new_param_coro(self) -> None:
        value1: int = await demo(1, 3)
        coro: Coroutine = demo(1, 5)
        value2: int = await asyncio_helper.gen_new_param_coro(coro, {"b": 3})
        coro.close()
        assert value1 == value2

    async def test_as_first_completed(self) -> None:
        def set_future_true(f: asyncio.Future) -> None:
            f.set_result(True)

        future1: asyncio.Future = asyncio.Future()
        future2: asyncio.Future = asyncio.Future()
        future3: asyncio.Future = asyncio.Future()
        loop: asyncio.AbstractEventLoop = asyncio_helper.get_event_loop()
        loop.call_later(0.1, set_future_true, future1)
        loop.call_later(10, set_future_true, future2)
        loop.call_later(10, set_future_true, future3)
        await asyncio_helper.as_first_completed([future1, future2], not_cancel_future_list=[future3])
        assert future1.done() and future2.cancelled() and not future3.done()

    async def test_del_future(self) -> None:
        future1: asyncio.Future = asyncio.Future()
        with pytest.raises(asyncio.CancelledError):
            asyncio_helper.del_future(future1)

        future2: asyncio.Future = asyncio.Future()
        asyncio_helper.safe_del_future(future2)

    async def test_semaphore(self) -> None:
        semaphore: asyncio_helper.Semaphore = asyncio_helper.Semaphore(100)
        async with semaphore:
            assert semaphore.inflight == 1


class TestAsyncioHelperDeadline:
    async def test_delay_is_none(self) -> None:
        deadline: asyncio_helper.Deadline = asyncio_helper.Deadline(None)
        timestamp: float = time.time()
        await deadline.wait_for(asyncio.sleep(0.1))
        assert 0.09 < time.time() - timestamp <= 0.11
        timestamp = time.time()
        with deadline:
            await asyncio.sleep(0.1)
        assert 0.09 < time.time() - timestamp <= 0.11

    async def test_deadline_timeout(self) -> None:
        with pytest.raises(asyncio.TimeoutError):
            deadline: asyncio_helper.Deadline = asyncio_helper.Deadline(0.1)
            with deadline:
                await asyncio.sleep(2)

    async def test_deadline_spec_timeout(self) -> None:
        exc: Exception = RuntimeError("test")
        deadline: asyncio_helper.Deadline = asyncio_helper.Deadline(0.5, timeout_exc=exc)
        with pytest.raises(RuntimeError) as e:
            with deadline:
                await asyncio.sleep(1)

        exec_msg = e.value.args[0]
        assert exec_msg == "test"

    async def test_deadline_ignore_exc(self) -> None:
        deadline: asyncio_helper.Deadline = asyncio_helper.Deadline(
            0.5, timeout_exc=asyncio_helper.IgnoreDeadlineTimeoutExc()
        )
        start_time: float = time.time()
        with deadline:
            await asyncio.sleep(1)
        assert 0.4 <= (time.time() - start_time) <= 0.6

    async def test_deadline_call_with_in_self_with_block(self) -> None:
        with pytest.raises(RuntimeError) as e:
            deadline: asyncio_helper.Deadline = asyncio_helper.Deadline(0.5)
            with deadline:
                with deadline:
                    await asyncio.sleep(1)

        exec_msg = e.value.args[0]
        assert exec_msg == "`with` can only be called once"

    async def test_deadline_wait_for(self) -> None:
        deadline: asyncio_helper.Deadline = asyncio_helper.Deadline(0.5)
        with pytest.raises(asyncio.TimeoutError):
            await deadline.wait_for(asyncio.Future())

        deadline = asyncio_helper.Deadline(0.5, timeout_exc=asyncio_helper.IgnoreDeadlineTimeoutExc())
        await deadline.wait_for(asyncio.Future())

        deadline = asyncio_helper.Deadline(0.5, timeout_exc=RuntimeError("test"))
        with pytest.raises(RuntimeError) as e:
            await deadline.wait_for(asyncio.Future())

        exec_msg = e.value.args[0]
        assert exec_msg == "test"

    async def test_deadline_await(self) -> None:
        start_time: float = asyncio.get_event_loop().time()
        await asyncio_helper.Deadline(0.5)
        assert 0.45 <= (asyncio.get_event_loop().time() - start_time) <= 0.55

    async def test_deadline_context(self) -> None:
        deadline: asyncio_helper.Deadline = asyncio_helper.Deadline(0.5)
        with deadline:
            assert deadline == asyncio_helper.deadline_context.get()
            with deadline.inherit() as child_deadline:
                context_deadline: Optional[asyncio_helper.Deadline] = asyncio_helper.deadline_context.get()
                assert context_deadline
                assert child_deadline == context_deadline
                assert deadline == context_deadline._parent
            assert deadline == asyncio_helper.deadline_context.get()

    async def test_repeat_call_with_in_func(self) -> None:
        deadline: asyncio_helper.Deadline = asyncio_helper.Deadline(0.5)
        with deadline:
            await asyncio.sleep(0.1)
        with deadline:
            await asyncio.sleep(0.1)
        with pytest.raises(asyncio.TimeoutError):
            with deadline:
                await asyncio.sleep(1)
