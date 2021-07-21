from typing import Any, AsyncGenerator, Callable, List


class BaseCoordinator(object):
    async def register(self, server_name: str, host: str, port: str) -> None:
        pass

    async def deregister(self, server_name: str, host: str, port: str) -> None:
        pass

    async def discovery(self, server_name: str) -> AsyncGenerator[dict, Any]:
        yield {}

    async def watch(self, server_name: str, put_callback: List[Callable], del_callback: List[Callable]) -> None:
        pass
