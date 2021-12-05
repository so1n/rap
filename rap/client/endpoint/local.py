import asyncio
from typing import TYPE_CHECKING, List, Optional

from rap.client.endpoint.base import BalanceEnum, BaseEndpoint

if TYPE_CHECKING:
    from rap.client.core import BaseClient


class LocalEndpoint(BaseEndpoint):
    """
    This endpoint only supports initializing transport based on parameters,
    and will not dynamically adjust transport at runtime
    """

    def __init__(
        self,
        conn_list: List[dict],
        app: "BaseClient",
        read_timeout: Optional[int] = None,
        declare_timeout: Optional[int] = None,
        ssl_crt_path: Optional[str] = None,
        pack_param: Optional[dict] = None,
        unpack_param: Optional[dict] = None,
        balance_enum: Optional[BalanceEnum] = None,
        min_ping_interval: Optional[int] = None,
        max_ping_interval: Optional[int] = None,
        ping_fail_cnt: Optional[int] = None,
        max_pool_size: Optional[int] = None,
        min_poll_size: Optional[int] = None,
    ):
        """
        :param conn_list: transport info list, 参数和默认值跟`BaseEndpoint.create`的参数保持一致
            like:[{"ip": localhost, "port": 9000, "weight": 10, "max_inflight": 100, "size": 2}]
        :param declare_timeout: declare timeout include request & response
        :param ssl_crt_path: client ssl crt file path
        :param balance_enum: balance pick transport method, default random
        :param pack_param: msgpack pack param
        :param unpack_param: msgpack unpack param
        :param min_ping_interval: send client ping min interval
        :param max_ping_interval: send client ping max interval
        :param ping_fail_cnt: How many times ping fails to judge as unavailable
        """
        self._conn_config_list: List[dict] = conn_list
        super().__init__(
            app,
            declare_timeout=declare_timeout,
            read_timeout=read_timeout,
            ssl_crt_path=ssl_crt_path,
            balance_enum=balance_enum,
            pack_param=pack_param,
            unpack_param=unpack_param,
            ping_fail_cnt=ping_fail_cnt,
            min_ping_interval=min_ping_interval,
            max_ping_interval=max_ping_interval,
            max_pool_size=max_pool_size,
            min_poll_size=min_poll_size,
        )

    async def start(self) -> None:
        """init transport and start"""
        if not self.is_close:
            raise ConnectionError(f"{self.__class__.__name__} is running")
        if not self._conn_config_list:
            raise ValueError("Can not found transport config")
        await asyncio.gather(
            *[
                self.create(
                    conn_config_dict["ip"],
                    conn_config_dict["port"],
                    weight=conn_config_dict.get("weight", None),
                    max_inflight=conn_config_dict.get("max_inflight", None),
                )
                for conn_config_dict in self._conn_config_list
            ]
        )
        await self._start()
