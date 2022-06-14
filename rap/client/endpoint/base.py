import asyncio
import logging
import random
from enum import Enum, auto
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Type

from rap.client.transport.pool import Pool
from rap.client.transport.transport import Transport
from rap.common.number_range import get_value_by_range

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
    def __init__(self, pool_list: List[Pool]):
        if not pool_list:
            raise ConnectionError("Endpoint Can not found available transport")
        self._pool_list: List[Pool] = pool_list
        self._lock: asyncio.Lock = asyncio.Lock()

    async def __aenter__(self) -> Transport:
        transport_list: List[Transport] = []
        for pool in self._pool_list:
            transport: Optional[Transport] = pool.transport
            if transport:
                transport_list.append(transport)
        if transport_list:
            transport: Optional[Transport] = max(transport_list, key=lambda x: x.pick_score)
        else:
            transport = None

        if transport:
            return transport
        else:
            # If no transport is available, transport is created from the first Pool
            async with self._lock:
                return await self._pool_list[0].new_transport()

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        return None


class PrivatePicker(Picker):
    async def __aenter__(self) -> Transport:
        self._pool: Pool = random.choice(self._pool_list)
        self._transport: Transport = await self._pool.fork_transport()
        return self._transport

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self._pool.absorption(self._transport)
        return None


class BaseEndpoint(object):
    def __init__(
        self,
        declare_timeout: Optional[int] = None,
        read_timeout: Optional[int] = None,
        ssl_crt_path: Optional[str] = None,
        balance_enum: Optional[BalanceEnum] = None,
        pack_param: Optional[dict] = None,
        unpack_param: Optional[dict] = None,
        min_ping_interval: Optional[int] = None,
        max_ping_interval: Optional[int] = None,
        ping_fail_cnt: Optional[int] = None,
        max_pool_size: Optional[int] = None,
        min_poll_size: Optional[int] = None,
        pool_class: Optional[Type[Pool]] = None,
    ) -> None:
        """
        :param declare_timeout: declare timeout include request & response, default 9
        :param ssl_crt_path: client ssl crt file path
        :param balance_enum: balance pick transport method, default random
        :param pack_param: msgpack pack param
        :param unpack_param: msgpack unpack param
        :param min_ping_interval: send client ping min interval, default 1
        :param max_ping_interval: send client ping max interval, default 3
        :param ping_fail_cnt: How many times ping fails to judge as unavailable, default 3
        :param pool_class: pool class
        """
        self._declare_timeout: int = declare_timeout or 9
        self._read_timeout: Optional[int] = read_timeout
        self._ssl_crt_path: Optional[str] = ssl_crt_path
        self._pack_param: Optional[dict] = pack_param
        self._unpack_param: Optional[dict] = unpack_param

        self._min_ping_interval: int = min_ping_interval or 1
        self._max_ping_interval: int = max_ping_interval or 3
        self._ping_fail_cnt: int = ping_fail_cnt or 3
        self._max_pool_size: int = max_pool_size or 3
        self._min_pool_size: int = min_poll_size or 1

        self._transport_key_list: List[Tuple[str, int]] = []
        self._transport_pool_dict: Dict[Tuple[str, int], Pool] = {}
        self._round_robin_index: int = 0

        self._run_event: asyncio.Event = asyncio.Event()
        self._pool_class: Type[Pool] = pool_class or Pool

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
        """create and init transport
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
        pool: Pool = self._pool_class(
            app,
            host=ip,
            port=port,
            weight=weight,
            ssl_crt_path=self._ssl_crt_path,
            pack_param=self._pack_param,
            unpack_param=self._unpack_param,
            max_inflight=max_inflight,
            read_timeout=self._read_timeout,
            declare_timeout=self._declare_timeout,
            min_ping_interval=self._min_ping_interval,
            max_ping_interval=self._max_ping_interval,
            ping_fail_cnt=self._ping_fail_cnt,
            max_pool_size=self._max_pool_size,
            min_pool_size=self._min_pool_size,
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
        picker_class = picker_class or Picker
        return picker_class(pool_list)

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
