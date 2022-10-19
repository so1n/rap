import asyncio
from typing import Optional, Tuple, Type

from typing_extensions import TypedDict

from rap.client.endpoint.base import BalanceEnum, BaseEndpoint, BaseEndpointProvider
from rap.client.transport.pool import PoolProvider


class ConnParamTypedDict(TypedDict, total=False):
    ip: str
    port: int
    weight: int
    max_inflight: int


class LocalEndpoint(BaseEndpoint):
    """
    This endpoint only supports initializing transport based on parameters,
    and will not dynamically adjust transport at runtime
    """

    def __init__(
        self,
        *conn_config: ConnParamTypedDict,
        balance_enum: Optional[BalanceEnum] = None,
        pool_provider: Optional[PoolProvider] = None,
    ):
        """
        :param conn_list: transport info list,
            like:[{"ip": localhost, "port": 9000, "weight": 10, "max_inflight": 100}]
        :param balance_enum: balance pick transport method, default random
        """
        self._conn_config_list: Tuple[ConnParamTypedDict, ...] = conn_config
        super().__init__(balance_enum=balance_enum, pool_provider=pool_provider)

    async def start(self) -> None:
        """init transport and start"""
        if not self.is_close:
            raise ConnectionError(f"{self.__class__.__name__} is running")
        if not self._conn_config_list:
            raise ValueError("Can not found transport config")
        await asyncio.gather(
            *[
                self.create(
                    conn_config_dict.get("ip", None),
                    conn_config_dict.get("port", None),
                    weight=conn_config_dict.get("weight", None),
                    max_inflight=conn_config_dict.get("max_inflight", None),
                )
                for conn_config_dict in self._conn_config_list
            ]
        )
        self._start()


class LocalEndpointProvider(BaseEndpointProvider):
    @classmethod
    def build(
        cls,
        *conn_config: ConnParamTypedDict,
        endpoint: Type[LocalEndpoint] = LocalEndpoint,
        balance_enum: Optional[BalanceEnum] = None,
    ) -> "LocalEndpointProvider":
        """
        :param conn_config: transport info list,
            like:[{"ip": localhost, "port": 9000, "weight": 10, "max_inflight": 100}]
        :param endpoint: LocalEndpoint class
        :param balance_enum: balance pick transport method, default random
        """
        return cls(endpoint, *conn_config, balance_enum=balance_enum)
