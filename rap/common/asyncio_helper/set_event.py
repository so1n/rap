import asyncio
from contextlib import contextmanager
from typing import Generic, Iterator, Set, TypeVar

_T = TypeVar("_T")


class SetEvent(Generic[_T]):
    """Combining the functions of set and asyncio.Event"""

    def __init__(self):
        self._event: asyncio.Event = asyncio.Event()
        self._set: Set[_T] = set()
        self.clear()

    @contextmanager
    def cm(self, item: _T) -> Iterator[None]:
        self.add(item)
        try:
            yield
        finally:
            self.remove(item)

    def clear(self) -> None:
        self._event.set()
        self._set.clear()

    ##############
    # set method #
    ##############
    def add(self, item: _T) -> None:
        self._set.add(item)
        self._event.clear()

    def remove(self, item: _T) -> None:
        self._set.remove(item)
        if not self._set:
            self._event.set()

    def __contains__(self, item: _T) -> bool:
        return item in self._set

    def __iter__(self) -> Iterator[_T]:
        return iter(self._set)

    ################
    # event method #
    ################
    def is_set(self) -> bool:
        return self._event.is_set()

    async def wait(self):
        await self._event.wait()
