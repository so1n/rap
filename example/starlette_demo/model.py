from typing import AsyncIterator


async def sync_sum(a: int, b: int) -> int:
    return 0


async def async_gen(a: int) -> AsyncIterator[int]:
    yield 0
