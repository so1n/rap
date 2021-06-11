from typing import List, Optional

from rap.client.endpoint.base import BaseEndpoint, SelectConnEnum
from rap.common.conn import Connection


class LocalEndpoint(BaseEndpoint):
    def __init__(
        self,
        server_name: str,
        conn_list: List[dict],
        timeout: int = 9,
        ssl_crt_path: Optional[str] = None,
        select_conn_method: SelectConnEnum = SelectConnEnum.random,
    ):
        """
        conn_list: [{
            "ip": "localhost",
            "port": 9000,
            "weight": 1,
            "min_weight": 0.1
        }]
        """
        self._conn_list: List[dict] = conn_list
        super().__init__(server_name, timeout, ssl_crt_path, select_conn_method)

    def get_conn(self) -> Connection:
        raise NotImplementedError

    async def start(self) -> None:
        if not self.is_close:
            raise ConnectionError(f"{self.__class__.__name__} is running")
        for conn_dict in self._conn_list:
            ip: str = conn_dict["ip"]
            port: int = conn_dict["port"]
            weight: int = conn_dict.get("weight", 1)
            min_weight: int = conn_dict.get("weight", 1)
            await self.create(ip, port, weight, min_weight)
