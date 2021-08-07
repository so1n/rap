from typing import List, Optional

from rap.client.endpoint.base import BaseEndpoint, SelectConnEnum


class LocalEndpoint(BaseEndpoint):
    """
    This endpoint only supports initializing conn based on parameters, and will not dynamically adjust conn at runtime
    """

    def __init__(
        self,
        server_name: str,
        conn_list: List[dict],
        timeout: Optional[int] = None,
        ssl_crt_path: Optional[str] = None,
        pack_param: Optional[dict] = None,
        unpack_param: Optional[dict] = None,
        select_conn_method: Optional[SelectConnEnum] = None,
    ):
        """
        server_name: server name
        conn_list: client conn info
          include ip, port, weight
          ip: server ip
          port: server port
          weight: select this conn weight
          e.g.  [{"ip": "localhost", "port": "9000", weight: 10}]
        timeout: read response from consumer timeout
        """
        self._conn_list: List[dict] = conn_list
        super().__init__(
            server_name, timeout, ssl_crt_path, select_conn_method, pack_param=pack_param, unpack_param=unpack_param
        )

    async def start(self) -> None:
        if not self.is_close:
            raise ConnectionError(f"{self.__class__.__name__} is running")
        for conn_dict in self._conn_list:
            ip: str = conn_dict["ip"]
            port: int = conn_dict["port"]
            weight: int = conn_dict.get("weight", 10)
            await self.create(ip, port, weight)
        await super().start()
