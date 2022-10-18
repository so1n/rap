import asyncio
import logging
import random
from enum import Enum, auto
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Type

from rap.client.transport.pool import Pool, PoolProvider, PoolWrapTransport
from rap.client.transport.transport import Transport
from rap.common.number_range import get_value_by_range
from rap.common.provider import Provider

logger: logging.Logger = logging.getLogger(__name__)
if TYPE_CHECKING:
    from rap.client.core import BaseClient


class BalanceEnum(Enum):
    """Balance method
    random: random pick a transport
    round_robin: round pick transport
    """

    random = auto()
    round_robin = auto()


class Picker(object):
    """Select the best transport from the connection pool"""

    _wrap_transport: PoolWrapTransport

    def __init__(self, pool: Pool):
        self._pool: Pool = pool

    async def __aenter__(self) -> Transport:
        self._wrap_transport = await self._pool.use_transport()
        return self._wrap_transport.transport

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self._pool.release_transport(self._wrap_transport)


class PrivatePicker(Picker):
    """provide a private transport"""

    async def __aenter__(self) -> Transport:
        self._transport: Transport = await self._pool.fork_transport()
        return self._transport

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self._pool.absorption(self._transport)
        return None


class BaseEndpoint(object):
    """
    Responsible for managing connections for multiple different service instances (these services function the same)
        Provides a variety of different load balancing implementations at the same time
    """

    def __init__(
        self,
        pool_provider: Optional[PoolProvider] = None,
        balance_enum: Optional[BalanceEnum] = None,
    ) -> None:
        """
        :param balance_enum: balance pick transport method, default random
        """
        self._pool_provider: PoolProvider = pool_provider or PoolProvider.build()
        self._transport_key_list: List[Tuple[str, int]] = []
        self._transport_pool_dict: Dict[Tuple[str, int], Pool] = {}
        self._round_robin_index: int = 0

        self._run_event: asyncio.Event = asyncio.Event()

        setattr(self, self._pick_pool.__name__, self._random_pick_pool)
        if balance_enum:
            if balance_enum == BalanceEnum.random:
                setattr(self, self._pick_pool.__name__, self._random_pick_pool)
            elif balance_enum == BalanceEnum.round_robin:
                setattr(self, self._pick_pool.__name__, self._round_robin_pick_pool)

    @property
    def is_close(self) -> bool:
        return not self._run_event.is_set()

    async def await_start(self) -> None:
        await self._run_event.wait()

    async def create(
        self,
        app: "BaseClient",
        ip: Optional[str] = None,
        port: Optional[int] = None,
        weight: Optional[int] = None,
        max_inflight: Optional[int] = None,
    ) -> None:
        """create and init pool
        :param app: client app
        :param ip: server ip
        :param port: server port
        :param weight: select transport weight
        :param max_inflight: Maximum number of connections per transport
        """
        ip = ip or "127.0.0.1"
        port = port or 9000
        key: Tuple[str, int] = (ip, port)
        if key in self._transport_pool_dict:
            return

        weight = get_value_by_range(weight, 0, 10) if weight else 10
        max_inflight = get_value_by_range(max_inflight, 0) if max_inflight else 100
        pool: Pool = self._pool_provider.create_instance(
            app, host=ip, port=port, weight=weight, max_inflight=max_inflight
        )
        self._transport_pool_dict[key] = pool
        self._transport_key_list.append(key)
        await pool.create()

    @staticmethod
    async def destroy(pool: Pool) -> None:
        await pool.destroy()

    def _start(self) -> None:
        self._run_event.set()

    async def start(self, app: "BaseClient") -> None:
        """start endpoint and create&init transport"""
        raise NotImplementedError

    async def stop(self) -> None:
        """stop endpoint and close all transport and cancel future"""
        while self._transport_key_list:
            transport_key: Tuple[str, int] = self._transport_key_list.pop()
            transport_pool: Optional[Pool] = self._transport_pool_dict.pop(transport_key, None)
            if transport_pool:
                await self.destroy(transport_pool)

        self._transport_key_list = []
        self._transport_pool_dict = {}
        self._run_event.clear()

    def picker(self, cnt: int = 3, picker_class: Optional[Type[Picker]] = None) -> Picker:
        """get transport by endpoint
        :param cnt: How many transport to get
        :param picker_class: Specific implementation of picker
        """
        if not self._transport_key_list:
            raise ConnectionError("Endpoint Can not found available transport")
        cnt = min(len(self._transport_key_list), cnt)
        pool_list: List[Pool] = self._pick_pool(cnt)
        if not pool_list:
            raise ConnectionError("Endpoint Can not found available transport")
        pool_list.sort(key=lambda x: x.pick_score, reverse=True)
        picker_class = picker_class or Picker
        return picker_class(pool_list[0])

    def _pick_pool(self, cnt: int) -> List[Pool]:
        """fake code"""
        return [transport_group for transport_group in self._transport_pool_dict.values()][:cnt]

    def _random_pick_pool(self, cnt: int) -> List[Pool]:
        """random get pool"""
        key_list: List[Tuple[str, int]] = random.choices(self._transport_key_list, k=cnt)
        return [self._transport_pool_dict[key] for key in key_list]

    def _round_robin_pick_pool(self, cnt: int) -> List[Pool]:
        """get pool by round robin"""
        self._round_robin_index += 1
        index: int = self._round_robin_index % (len(self._transport_key_list))
        key_list: List[Tuple[str, int]] = self._transport_key_list[index : index + cnt]
        return [self._transport_pool_dict[key] for key in key_list]

    def __len__(self) -> int:
        return len(self._transport_key_list)


class BaseEndpointProvider(Provider):
    def inject(
        self,
        pool_provider: Optional[PoolProvider] = None,
    ) -> "BaseEndpointProvider":
        self._kwargs["pool_provider"] = pool_provider or PoolProvider.build()
        return self

    @classmethod
    def build(
        cls,
        *args,
        **kwargs,
    ) -> "BaseEndpointProvider":
        raise NotImplementedError
