import asyncio
import logging
import random
import time
from enum import Enum, auto
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from rap.client.transport.transport import Transport, TransportGroup
from rap.common.asyncio_helper import Deadline, IgnoreDeadlineTimeoutExc
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
    def __init__(self, transport_list: List[Transport]):
        if not transport_list:
            raise ConnectionError("Endpoint Can not found available transport")
        self._transport: Transport = max(transport_list, key=lambda x: x.pick_score)

    async def __aenter__(self) -> Transport:
        await self._transport.semaphore.acquire()
        return self._transport

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self._transport.semaphore.release()
        return None


class PrivatePicker(Picker):
    def __init__(self, endpoint: "BaseEndpoint", transport_list: List[Transport]):
        self._endpoint: BaseEndpoint = endpoint
        super().__init__(transport_list)

    async def __aenter__(self) -> Transport:
        self._transport = await self._endpoint.create_one(
            self._transport.host, self._transport.port, self._transport.weight, self._transport.semaphore.raw_value
        )
        return self._transport

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self._transport.await_close()
        return None


class BaseEndpoint(object):
    def __init__(
        self,
        app: "BaseClient",
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
    ) -> None:
        """
        :param app: client app
        :param declare_timeout: declare timeout include request & response, default 9
        :param ssl_crt_path: client ssl crt file path
        :param balance_enum: balance pick transport method, default random
        :param pack_param: msgpack pack param
        :param unpack_param: msgpack unpack param
        :param min_ping_interval: send client ping min interval, default 1
        :param max_ping_interval: send client ping max interval, default 3
        :param ping_fail_cnt: How many times ping fails to judge as unavailable, default 3
        """
        self._app: "BaseClient" = app
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

        self._connected_cnt: int = 0
        self._transport_key_list: List[Tuple[str, int]] = []
        self._transport_group_dict: Dict[Tuple[str, int], TransportGroup] = {}
        self._round_robin_index: int = 0

        self._run_event: asyncio.Event = asyncio.Event()

        setattr(self, self._pick_transport.__name__, self._random_pick_transport)
        if balance_enum:
            if balance_enum == BalanceEnum.random:
                setattr(self, self._pick_transport.__name__, self._random_pick_transport)
            elif balance_enum == BalanceEnum.round_robin:
                setattr(self, self._pick_transport.__name__, self._round_robin_pick_transport)

    async def _ping_event(self, transport: Transport) -> None:
        """client ping-pong handler, check transport is available"""
        ping_fail_interval: int = int(self._max_ping_interval * self._ping_fail_cnt)
        while True:
            available: bool = (time.time() - transport.last_ping_timestamp) < ping_fail_interval
            logger.debug("transport:%s available:%s rtt:%s", transport.connection_info, available, transport.rtt)
            transport_group: TransportGroup = self._transport_group_dict[(transport.host, transport.port)]
            if not available:
                logger.error(f"ping {transport.sock_tuple} timeout... exit")
                return
            elif not (self._min_ping_interval == 1 and self._max_ping_interval == 1):
                transport.inflight_load.append(transport.semaphore.inflight)
                # Simple design, don't want to use pandas&numpy in the app framework
                avg_inflight: float = sum(transport.inflight_load) / len(transport.inflight_load)
                if avg_inflight > 80 and len(transport_group) < self._max_pool_size:
                    # The current transport is under too much pressure and
                    # a transport needs to be created to divert the traffic
                    await self.create(
                        transport.peer_tuple[0],
                        transport.peer_tuple[1],
                        transport.weight,
                        transport.semaphore.raw_value,
                    )
                elif avg_inflight < 20 and len(transport_group) > self._min_pool_size:
                    # When transport is just created, inflight is 5.
                    # The current transport is not handling too many requests and needs to lower its priority
                    transport.available_level -= 1
                elif transport.available and transport.available_level < 5:
                    # If the current transport load is moderate and the
                    # priority is not optimal (priority less than 5), then increase its priority.
                    transport.available_level += 1

            if transport.available_level <= 0 and transport.available:
                # The current transport availability level is 0,
                # which means that it is not available and will be marked as unavailable
                # and will be automatically closed later.

                # transport.close_soon()
                transport.available = False
                return

            logger.debug(
                "transport:%s available:%s rtt:%s", transport.peer_tuple, transport.available_level, transport.rtt
            )
            next_ping_interval: int = random.randint(self._min_ping_interval, self._max_ping_interval)
            try:
                with Deadline(next_ping_interval, timeout_exc=IgnoreDeadlineTimeoutExc()) as d:
                    await transport.ping()
                    await transport.sleep_and_listen(d.surplus)
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.debug(f"{transport.connection_info} ping event error:{e}")

    @property
    def is_close(self) -> bool:
        return not self._run_event.is_set()

    async def await_start(self) -> None:
        await self._run_event.wait()

    async def create_one(
        self,
        ip: str,
        port: int,
        weight: int,
        max_inflight: int,
    ) -> Transport:
        key: Tuple[str, int] = (ip, port)
        transport: Transport = Transport(
            self._app,
            ip,
            port,
            weight,
            ssl_crt_path=self._ssl_crt_path,
            pack_param=self._pack_param,
            unpack_param=self._unpack_param,
            max_inflight=max_inflight,
        )

        def _transport_done(f: asyncio.Future) -> None:
            self._connected_cnt -= 1
            try:
                transport_group: Optional[TransportGroup] = self._transport_group_dict.get(key, None)
                if transport_group:
                    transport_group.remove(transport)
                if transport_group is None or len(transport_group) == 0:
                    self._transport_key_list.remove(key)
            except ValueError:
                pass
            except Exception as _e:
                msg: str = f"close transport error: {_e}"
                if f.exception():
                    msg += f", transport done exc:{f.exception()}"
                logger.exception(msg)

        try:
            with Deadline(self._declare_timeout, timeout_exc=asyncio.TimeoutError(f"transport:{key} declare timeout")):
                await transport.connect()
        except Exception as e:
            await transport.await_close()
            raise e
        self._connected_cnt += 1
        transport.listen_future.add_done_callback(lambda f: _transport_done(f))
        transport.ping_future = asyncio.ensure_future(self._ping_event(transport))
        transport.ping_future.add_done_callback(lambda f: transport.close())
        return transport

    async def create(
        self,
        ip: str,
        port: int,
        weight: Optional[int] = None,
        max_inflight: Optional[int] = None,
    ) -> None:
        """create and init transport
        :param ip: server ip
        :param port: server port
        :param weight: select transport weight
        :param max_inflight: Maximum number of connections per transport
        """
        weight = get_value_by_range(weight, 0, 10) if weight else 10
        max_inflight = get_value_by_range(max_inflight, 0) if max_inflight else 100

        key: Tuple[str, int] = (ip, port)
        if key not in self._transport_group_dict:
            self._transport_group_dict[key] = TransportGroup()
            self._transport_key_list.append(key)

        transport_group_len: int = len(self._transport_group_dict[key])
        if transport_group_len >= self._max_pool_size:
            return
        if not self._transport_group_dict[key]:
            create_size = self._min_pool_size
        elif transport_group_len > self._min_pool_size:
            create_size = 1
        else:
            create_size = self._min_pool_size - transport_group_len

        for _ in range(create_size):
            transport: Transport = await self.create_one(
                ip,
                port,
                weight,
                max_inflight=max_inflight,
            )
            self._transport_group_dict[key].add(transport)

    @staticmethod
    async def destroy(transport_group: TransportGroup) -> None:
        await transport_group.destroy()

    def _start(self) -> None:
        self._run_event.set()

    async def start(self) -> None:
        """start endpoint and create&init transport"""
        raise NotImplementedError

    async def stop(self) -> None:
        """stop endpoint and close all transport and cancel future"""
        while self._transport_key_list:
            await self.destroy(self._transport_group_dict[self._transport_key_list.pop()])

        self._transport_key_list = []
        self._transport_group_dict = {}
        self._run_event.clear()

    def picker(self, cnt: int = 3, is_private: Optional[bool] = None) -> Picker:
        """get transport by endpoint
        :param cnt: How many transport to get
        :param is_private: If the value is True, it will get transport for its own use only. default False
        """
        if not self._transport_key_list:
            raise ConnectionError("Endpoint Can not found available transport")
        cnt = min(self._connected_cnt, cnt)
        transport_list: List[Transport] = self._pick_transport(cnt)
        if not transport_list:
            raise ConnectionError("Endpoint Can not found available transport")
        if is_private:
            return PrivatePicker(endpoint=self, transport_list=transport_list)
        else:
            return Picker(transport_list)

    def _pick_transport(self, cnt: int) -> List[Transport]:
        """fake code"""
        return [
            transport_group.transport
            for transport_group in self._transport_group_dict.values()
            if transport_group.transport
        ]

    def _random_pick_transport(self, cnt: int) -> List[Transport]:
        """random get transport"""
        key_list: List[Tuple[str, int]] = random.choices(self._transport_key_list, k=cnt)
        transport_list: List[Transport] = []
        for key in key_list:
            transport: Optional[Transport] = self._transport_group_dict[key].transport
            if transport:
                transport_list.append(transport)
        return transport_list

    def _round_robin_pick_transport(self, cnt: int) -> List[Transport]:
        """get transport by round robin"""
        self._round_robin_index += 1
        index: int = self._round_robin_index % (len(self._transport_key_list))
        key_list: List[Tuple[str, int]] = self._transport_key_list[index : index + cnt]
        transport_list: List[Transport] = []
        for key in key_list:
            transport: Optional[Transport] = self._transport_group_dict[key].transport
            if transport:
                transport_list.append(transport)
        return transport_list

    def __len__(self) -> int:
        return self._connected_cnt
