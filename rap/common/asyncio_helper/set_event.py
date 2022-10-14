import asyncio
from contextlib import contextmanager
from typing import Generic, Iterator, Set, TypeVar

_T = TypeVar("_T")


class BaseSetEvent(Generic[_T]):
    """Combining the functions of set and asyncio.Event"""

    def __init__(self):
        self._event: asyncio.Event = asyncio.Event()
        self._set: Set[_T] = set()

    @contextmanager
    def cm(self, item: _T) -> Iterator[None]:
        self.add(item)
        try:
            yield
        finally:
            self.remove(item)

    def clear(self) -> None:
        self._set.clear()
        self._set_remove_hook()

    ##############
    # set method #
    ##############
    def _set_add_hook(self) -> None:
        raise NotImplementedError()

    def _set_remove_hook(self) -> None:
        raise NotImplementedError()

    def add(self, item: _T) -> None:
        self._set.add(item)
        self._set_add_hook()

    def remove(self, item: _T) -> None:
        self._set.remove(item)
        self._set_remove_hook()

    def pop(self) -> _T:
        result: _T = self._set.pop()
        self._set_remove_hook()
        return result

    def __contains__(self, item: _T) -> bool:
        return item in self._set

    def __iter__(self) -> Iterator[_T]:
        return iter(self._set)

    def __len__(self) -> int:
        return len(self._set)

    ################
    # event method #
    ################
    def is_set(self) -> bool:
        return self._event.is_set()

    async def wait(self):
        await self._event.wait()


class SetEvent(BaseSetEvent[_T]):
    """Combining the functions of set and asyncio.Event"""

    def __init__(self):
        super().__init__()
        self._event.set()

    def _set_add_hook(self):
        self._event.clear()

    def _set_remove_hook(self):
        if not self._set:
            self._event.set()

    async def wait_empty(self) -> None:
        await self.wait()


class ReversalSetEvent(BaseSetEvent[_T]):
    def __init__(self):
        super().__init__()
        self._event.clear()

    def _set_add_hook(self):
        self._event.set()

    def _set_remove_hook(self):
        if not self._set:
            self._event.clear()

    async def wait_set(self) -> None:
        await self.wait()
