from typing import Any, AsyncGenerator


class BaseCoordinator(object):
    async def stop(self) -> None:
        pass

    async def register(self, server_name: str, host: str, port: str, weight: int) -> None:
        pass

    async def deregister(self, server_name: str, host: str, port: str) -> None:
        pass

    async def discovery(self, server_name: str) -> AsyncGenerator[dict, Any]:
        yield {}
