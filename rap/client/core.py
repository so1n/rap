import inspect

from functools import wraps
from typing import Any, Callable, cast, List, Optional, Tuple

from rap.client.model import Response
from rap.client.transport import Transport
from rap.common.conn import Connection
from rap.common.middleware import BaseMiddleware


__all__ = ["Client"]


class AsyncIteratorCall:
    """client support async iterator"""

    def __init__(self, method: str, client: "Client", *args: Tuple):
        self._method: str = method
        self._call_id: Optional[int] = None
        self._args = args
        self._client: "Client" = client
        self._conn: Connection = self._client.transport.now_conn

    def __aiter__(self):
        return self

    async def __anext__(self):
        """
        The server will return the call id of the generator function,
        and the client can continue to get data based on the call id.
        If no data, the server will return header.status_code = 301 and client must raise StopAsyncIteration Error.
        """
        response: Response = await self._client.transport.request(
            self._method, *self._args, call_id=self._call_id, conn=self._conn
        )
        self._call_id = response.body["call_id"]
        if response.header["status_code"] == 301:
            raise StopAsyncIteration()
        return response.body["result"]


class Client:
    def __init__(
        self,
        host_list: Optional[List[str]] = None,
        timeout: int = 9,
        keep_alive_time: int = 1200,
        ssl_crt_path: Optional[str] = None,
    ):
        if not host_list:
            host_list = ["localhost:9000"]
        self.transport: Transport = Transport(
            host_list,
            timeout=timeout,
            keep_alive_time=keep_alive_time,
            ssl_crt_path=ssl_crt_path
        )

    ##################
    # connect& close #
    ##################
    async def wait_close(self):
        """close client"""
        await self.transport.wait_close()

    async def connect(self):
        """
        Create&conn connection;
        start listen response;
        send declare msg to server
        """
        await self.transport.connect()

    def load_middleware(self, middleware_list: List[BaseMiddleware]):
        self.transport.load_middleware(middleware_list)

    # register
    def _async_register(self, func: Callable):
        """Decorate normal function"""

        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            return await self.raw_call(func.__name__, *args)

        return cast(Callable, wrapper)

    def _async_gen_register(self, func: Callable):
        """Decoration generator function"""
        @wraps(func)
        async def wrapper(*args, **kwargs) -> Any:
            async for result in self.iterator_call(func.__name__, *args):
                yield result

        return cast(Callable, wrapper)

    # client api
    async def raw_call(self, method: str, *args: Any) -> Any:
        """rpc client base call method"""
        response = await self.transport.request(method, *args)
        return response.body["result"]

    async def call(self, func: Callable, *args: Any) -> Any:
        """automatically resolve function names and call call_by_text"""
        return await self.raw_call(func.__name__, *args)

    async def iterator_call(self, method: str, *args: Any) -> Any:
        """Python-specific generator call"""
        async for result in AsyncIteratorCall(method, self, *args):
            yield result

    def register(self, func: Callable) -> Any:
        """Using this method to decorate a fake function can help you use it better.
        (such as ide completion, ide reconstruction and type hints)"""
        if inspect.iscoroutinefunction(func):
            return self._async_register(func)
        elif inspect.isasyncgenfunction(func):
            return self._async_gen_register(func)
