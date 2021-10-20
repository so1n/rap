from typing import TYPE_CHECKING, Any, Optional, Sequence

from rap.client.model import Response
from rap.common.conn import Connection

if TYPE_CHECKING:
    from rap.client.core import BaseClient


class AsyncIteratorCall:
    """let client support async iterator (keep sending and receiving messages under the same conn)"""

    def __init__(
        self,
        name: str,
        client: "BaseClient",
        conn: Connection,
        arg_param: Sequence[Any],
        header: Optional[dict] = None,
        group: Optional[str] = None,
    ):
        """
        :param name: func name
        :param client: rap base client
        :param arg_param: rpc func param
        :param group: func's group, default value is `default`
        :param header: request header
        """
        self._name: str = name
        self._client: "BaseClient" = client
        self._conn: Connection = conn
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
        response: Response = await self._client.transport.request(
            self._name,
            self._conn,
            arg_param=self._arg_param,
            call_id=self._call_id,
            header=self._header,
            group=self.group,
        )
        if response.status_code == 301:
            raise StopAsyncIteration()
        self._call_id = response.body["call_id"]
        return response.body["result"]
