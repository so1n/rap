import asyncio
import logging
import random
import time
from collections import deque
from typing import TYPE_CHECKING, Deque, Optional

from rap.client.transport.transport import Transport
from rap.common.asyncio_helper import Deadline, IgnoreDeadlineTimeoutExc, done_future, safe_del_future
from rap.common.number_range import get_value_by_range

logger: logging.Logger = logging.getLogger(__name__)
if TYPE_CHECKING:
    from rap.client.core import BaseClient


class Pool(object):
    def __init__(
        self,
        app: "BaseClient",
        host: str,
        port: int,
        weight: int,
        ssl_crt_path: Optional[str] = None,
        pack_param: Optional[dict] = None,
        unpack_param: Optional[dict] = None,
        max_inflight: Optional[int] = None,
        read_timeout: Optional[int] = None,
        declare_timeout: Optional[int] = None,
        min_ping_interval: Optional[int] = None,
        max_ping_interval: Optional[int] = None,
        ping_fail_cnt: Optional[int] = None,
        max_pool_size: Optional[int] = None,
        min_pool_size: Optional[int] = None,
    ) -> None:
        self._app: "BaseClient" = app
        self._host: str = host
        self._port: int = port
        self._weight: int = weight
        self._ssl_crt_path: Optional[str] = ssl_crt_path
        self._pack_param: Optional[dict] = pack_param
        self._unpack_param: Optional[dict] = unpack_param
        self._max_inflight: Optional[int] = max_inflight
        self._read_timeout: Optional[int] = read_timeout
        self._declare_timeout: int = declare_timeout or 9
        self._min_ping_interval: int = min_ping_interval or 1
        self._max_ping_interval: int = max_ping_interval or 3
        self._ping_fail_cnt: int = ping_fail_cnt or 3
        self._max_pool_size: int = max_pool_size or 3
        self._min_pool_size: int = min_pool_size or 1

        self._transport_deque: Deque[Transport] = deque()
        self._transport_manager_future: asyncio.Future = done_future()
        self._transport_manager_event: asyncio.Event = asyncio.Event()
        self._expected_number_of_transports: int = 0

    @property
    def is_close(self):
        return self._transport_manager_future.done()

    async def _transport_manger(self) -> None:
        """Ensure that the number of transports is adjusted to the desired value during the Pool run"""
        self._transport_manager_event.set()
        while True:
            await self._transport_manager_event.wait()
            transport_len: int = len(self._transport_deque)
            self._expected_number_of_transports = get_value_by_range(
                self._expected_number_of_transports, 0, self._max_pool_size
            )
            if transport_len < self._expected_number_of_transports:
                for _ in range(self._expected_number_of_transports - transport_len):
                    try:
                        await self._create_one_transport()
                    except Exception as e:
                        logger.warning(f"ignore transport manger create {self._host}:{self._port} error:{e}")
            elif transport_len > self._expected_number_of_transports:
                for _ in range(transport_len - self._expected_number_of_transports):
                    try:
                        await self._transport_deque.popleft().await_close()
                    except Exception as e:
                        logger.warning(f"ignore transport manger close {self._host}:{self._port} error:{e}")
            elif transport_len == 0 and self._expected_number_of_transports == 0:
                return
            elif transport_len == self._expected_number_of_transports:
                self._transport_manager_event.clear()

    async def _ping_handle(self, transport: Transport) -> None:
        """client ping-pong handler, check transport is available"""
        ping_fail_interval: int = int(self._max_ping_interval * self._ping_fail_cnt)

        def _change_transport_available() -> None:
            available: bool = (time.time() - transport.last_ping_timestamp) < ping_fail_interval
            logger.debug("transport:%s available:%s rtt:%s", transport.connection_info, available, transport.rtt)
            if not available:
                transport.available = False
                msg: str = f"ping {transport.sock_tuple} timeout... exit"
                logger.error(msg)
                raise RuntimeError(msg)
            elif not (self._min_ping_interval == 1 and self._max_ping_interval == 1):
                transport.inflight_load.append(transport.inflight)
                # Simple design, don't want to use pandas&numpy in the app framework
                avg_inflight: float = sum(transport.inflight_load) / len(transport.inflight_load)
                if avg_inflight > 80 and len(self) < self._max_pool_size:
                    # The current transport is under too much pressure and
                    # transport needs to be created to divert the traffic
                    self._expected_number_of_transports += 1
                    self._transport_manager_event.set()
                elif avg_inflight < 20 and len(self) > self._min_pool_size:
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
                transport.available = False
                raise RuntimeError("The current transport is idle and needs to be closed")

        try:
            while True:
                logger.debug(
                    "transport:%s available:%s rtt:%s", transport.peer_tuple, transport.available_level, transport.rtt
                )
                _change_transport_available()
                next_ping_interval: int = random.randint(self._min_ping_interval, self._max_ping_interval)
                try:
                    with Deadline(next_ping_interval, timeout_exc=IgnoreDeadlineTimeoutExc()) as d:
                        await transport.ping()
                        await transport.sleep_and_listen(d.surplus)
                except asyncio.CancelledError:
                    return
                except Exception as e:
                    logger.debug(f"{transport.connection_info} ping event error:{e}")
        finally:
            transport.close()

    async def _create_one_transport(self) -> None:
        transport: Transport = Transport(
            self._app,
            self._host,
            self._port,
            self._weight,
            ssl_crt_path=self._ssl_crt_path,
            pack_param=self._pack_param,
            unpack_param=self._unpack_param,
            max_inflight=self._max_inflight,
        )

        try:
            with Deadline(self._declare_timeout, timeout_exc=asyncio.TimeoutError("transport declare timeout")):
                await transport.connect()
        except Exception as e:
            if not transport.is_closed():
                try:
                    await transport.await_close()
                except Exception as close_e:
                    logger.error(f"ignore {transport.connection_info} close error:{close_e}")
            raise e
        ping_future: asyncio.Future = asyncio.create_task(self._ping_handle(transport))
        transport.listen_future.add_done_callback(lambda _: safe_del_future(ping_future))
        self._transport_deque.append(transport)
        logger.debug("create transport:%s", transport.connection_info)

    @property
    def transport(self) -> Optional[Transport]:
        self._transport_deque.rotate(1)
        while True:
            if not self._transport_deque:
                return None
            transport: Transport = self._transport_deque[0]
            if not transport.available:
                # pop transport and close
                self._transport_deque.popleft().close_soon()
                self._expected_number_of_transports -= 1
            else:
                return transport

    async def create(self) -> None:
        """
        Initialize the transport pool,
        normally the number of generated transport is less than or equal to min pool size
        """
        if not self.is_close:
            return
        transport_group_len: int = len(self)
        if transport_group_len >= self._max_pool_size:
            return
        elif transport_group_len == 0:
            self._expected_number_of_transports = self._min_pool_size
        else:
            self._expected_number_of_transports = self._min_pool_size - transport_group_len

        # Create one first to ensure that a proper link can be established
        await self._create_one_transport()
        # Number of links maintained in the back office
        self._transport_manager_future = asyncio.create_task(self._transport_manger())

    async def destroy(self) -> None:
        self._expected_number_of_transports = 0
        self._transport_manager_event.set()
        await self._transport_manager_future

    def __len__(self) -> int:
        return len(self._transport_deque)
