import asyncio
from typing import List, Optional

from rap.client.endpoint.base import BalanceEnum, BaseEndpoint
from rap.client.transport.transport import Transport


class LocalEndpoint(BaseEndpoint):
    """
    This endpoint only supports initializing conn based on parameters, and will not dynamically adjust conn at runtime
    """

    def __init__(
        self,
        conn_list: List[dict],
        transport: Transport,
        declare_timeout: Optional[int] = None,
        ssl_crt_path: Optional[str] = None,
        pack_param: Optional[dict] = None,
        unpack_param: Optional[dict] = None,
        balance_enum: Optional[BalanceEnum] = None,
        min_ping_interval: Optional[int] = None,
        max_ping_interval: Optional[int] = None,
        ping_fail_cnt: Optional[int] = None,
        wait_server_recover: bool = True,
    ):
        """
        :param conn_list: conn info list, 参数和默认值跟`BaseEndpoint.create`的参数保持一致
            like:[{"ip": localhost, "port": 9000, "weight": 10, "max_conn_inflight": 100, "size": 2}]
        :param transport: client transport
        :param declare_timeout: declare timeout include request & response
        :param ssl_crt_path: client ssl crt file path
        :param balance_enum: balance pick conn method, default random
        :param pack_param: msgpack pack param
        :param unpack_param: msgpack unpack param
        :param min_ping_interval: send client ping min interval
        :param max_ping_interval: send client ping max interval
        :param ping_fail_cnt: How many times ping fails to judge as unavailable
        :param wait_server_recover: If False, ping failure will close conn
        """
        self._conn_config_list: List[dict] = conn_list
        super().__init__(
            transport,
            declare_timeout=declare_timeout,
            ssl_crt_path=ssl_crt_path,
            balance_enum=balance_enum,
            pack_param=pack_param,
            unpack_param=unpack_param,
            ping_fail_cnt=ping_fail_cnt,
            min_ping_interval=min_ping_interval,
            max_ping_interval=max_ping_interval,
            wait_server_recover=wait_server_recover,
        )

    async def start(self) -> None:
        """init conn and start"""
        if not self.is_close:
            raise ConnectionError(f"{self.__class__.__name__} is running")
        if not self._conn_config_list:
            raise ValueError("Can not found conn config")
        await asyncio.gather(
            *[
                self.create(
                    conn_config_dict["ip"],
                    conn_config_dict["port"],
                    weight=conn_config_dict.get("weight", None),
                    size=conn_config_dict.get("size", None),
                    max_conn_inflight=conn_config_dict.get("max_conn_inflight", None),
                )
                for conn_config_dict in self._conn_config_list
            ]
        )
        await self._start()
