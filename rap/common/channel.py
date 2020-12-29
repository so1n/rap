from typing import Any, Tuple


class BaseChannel(object):
    async def loop(self, flag: bool = True) -> bool:
        raise NotImplementedError

    async def read(self) -> Any:
        raise NotImplementedError

    async def read_body(self):
        raise NotImplementedError

    async def write(self, body: Any):
        raise NotImplementedError

    async def close(self):
        raise NotImplementedError

    @property
    def is_close(self) -> bool:
        raise NotImplementedError

    def __aiter__(self) -> "BaseChannel":
        raise NotImplementedError

    async def __anext__(self):
        raise NotImplementedError
