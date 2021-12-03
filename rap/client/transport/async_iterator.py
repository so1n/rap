from typing import Any, Optional, Sequence

from rap.client.model import Response
from rap.client.transport.transport import Transport


class AsyncIteratorCall:
    """let client support async iterator (keep sending and receiving messages under the same transport)"""

    def __init__(
        self,
        name: str,
        transport: Transport,
        arg_param: Sequence[Any],
        header: Optional[dict] = None,
        group: Optional[str] = None,
    ):
        """
        :param name: func name
        :param transport: transport
        :param arg_param: rpc func param
        :param group: func's group, default value is `default`
        :param header: request header
        """
        self._name: str = name
        self._transport: Transport = transport
        self._call_id: Optional[int] = None
        self._arg_param: Sequence[Any] = arg_param
        self._header: Optional[dict] = header or {}
        self.group: Optional[str] = group

    #####################
    # async for support #
    #####################
    def __aiter__(self) -> "AsyncIteratorCall":
        return self

    async def __anext__(self) -> Any:
        """
        The server will return the invoke id of the generator function,
        and the client can continue to get data based on the invoke id.
        If no data, the server will return status_code = 301 and client must raise StopAsyncIteration Error.
        """
        response: Response = await self._transport.request(
            self._name,
            arg_param=self._arg_param,
            call_id=self._call_id,
            header=self._header,
            group=self.group,
        )
        if response.status_code == 301:
            raise StopAsyncIteration()
        self._call_id = response.body["call_id"]
        return response.body["result"]
