import asyncio
from typing import Optional


class Semaphore(asyncio.Semaphore):
    """Compared with the original version, an additional method `inflight` is used to obtain the current usage"""

    def __init__(self, value: int = 1, *, loop: Optional[asyncio.AbstractEventLoop] = None):
        self.raw_value: int = value
        super(Semaphore, self).__init__(value, loop=loop)

    @property
    def inflight(self) -> int:
        value: int = self.raw_value - self._value  # type: ignore
        if value < 0:
            value = 0
        if value > self.raw_value:
            value = self.raw_value
        return value
