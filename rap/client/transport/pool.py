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
        transport_age: Optional[int] = None,
        transport_age_jitter: Optional[int] = None,
        transport_max_age: Optional[int] = None,
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

        if transport_age and not transport_age_jitter:
            raise ValueError("transport_age_jitter must be set if transport_age is set")
        self._transport_age: Optional[int] = transport_age
        self._transport_age_jitter: Optional[int] = transport_age_jitter
        self._transport_max_age: int = transport_max_age or ((transport_age or 0) + 3600)

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
            # Make sure you can run it every once in a while and clear out unavailable transports in time
            await asyncio.wait([self._transport_manager_event.wait()], timeout=60)

            transport_len: int = len(self._transport_deque)
            self._expected_number_of_transports = get_value_by_range(
                self._expected_number_of_transports, 0, self._max_pool_size
            )

            for _ in range(transport_len):
                self._transport_deque.rotate(1)
                if not self._transport_deque[0].available:
                    try:
                        self._transport_deque.popleft().grace_close()
                    except Exception as e:
                        logger.warning(f"ignore transport manger close {self._host}:{self._port} error:{e}")

            if transport_len > self._expected_number_of_transports:
                for _ in range(transport_len - self._expected_number_of_transports):
                    try:
                        self._transport_deque.popleft().grace_close()
                    except Exception as e:
                        logger.warning(f"ignore transport manger close {self._host}:{self._port} error:{e}")

            if transport_len < self._expected_number_of_transports:
                for _ in range(self._expected_number_of_transports - transport_len):
                    try:
                        await self.new_transport()
                    except Exception as e:
                        logger.warning(f"ignore transport manger create {self._host}:{self._port} error:{e}")

            if transport_len == 0 and self._expected_number_of_transports == 0:
                return
            elif transport_len == self._expected_number_of_transports:
                # If the number of transports is the same as the desired number, wait notify
                self._transport_manager_event.clear()

    async def _ping_handle(self, transport: Transport) -> None:
        """client ping-pong handler, check transport is available"""
        ping_fail_interval: int = int(self._max_ping_interval * self._ping_fail_cnt)
        if self._transport_age and self._transport_age_jitter:
            transport_end_time: float = time.time() + int(
                self._transport_age + random.randint(0, self._transport_age_jitter)
            )
        else:
            transport_end_time = 0

        async def _change_transport_available() -> None:
            now_time: float = time.time()
            if transport_end_time and now_time > transport_end_time:
                if transport.available:
                    # When the transport time exceeds the limit, need to set the transport to become unavailable,
                    # so that it will not accept new requests, and wait for the pool manager to reorganize the transport
                    transport.available = False
                    self._transport_manager_event.set()
                else:
                    # If the transport exceeds the specified maximum survival time,
                    # the transport will be closed immediately
                    if now_time > transport_end_time + self._transport_max_age:
                        await transport.await_close()
                return
            available: bool = (now_time - transport.last_ping_timestamp) < ping_fail_interval
            logger.debug("transport:%s available:%s rtt:%s", transport.connection_info, available, transport.rtt)
            if not available:
                transport.available = False
                msg: str = f"ping {transport.sock_tuple} timeout... exit"
                logger.error(msg)
                raise RuntimeError(msg)
            elif not (self._min_ping_interval == 1 and self._max_ping_interval == 1):
                transport.inflight_load.append(int(transport.inflight / transport.raw_inflight * 100))
                # Simple design, don't want to use pandas&numpy in the app framework
                median_inflight: int = sorted(transport.inflight_load)[len(transport.inflight_load) // 2 + 1]
                if median_inflight > 80 and len(self) < self._max_pool_size:
                    # The current transport is under too much pressure and
                    # transport needs to be created to divert the traffic
                    self._expected_number_of_transports += 1
                    self._transport_manager_event.set()
                elif median_inflight < 20 and len(self) > self._min_pool_size:
                    # When transport is just created, inflight is 5.
                    # The current transport is not handling too many requests and needs to lower its priority
                    transport.available_level -= 1
                elif transport.available and transport.available_level < 5:
                    # If the current transport load is moderate and the
                    # priority is not optimal (priority less than 5), then increase its priority.
                    transport.available_level += 1

            if transport.available_level <= 0 and transport.available:
                # The current transport availability level is 0, which means that it is not available
                raise RuntimeError("The current transport is idle and needs to be closed")

        try:
            while True:
                logger.debug(
                    "transport:%s available:%s rtt:%s", transport.peer_tuple, transport.available_level, transport.rtt
                )
                await _change_transport_available()
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
            # Mark the transport as unavailable so that it will be called off the next time the transport is fetched
            transport.available = False
            self._expected_number_of_transports -= 1
            self._transport_manager_event.set()

    def absorption(self, transport: Transport) -> None:
        """Absorb the transport into the pool."""
        if getattr(transport, "is_absorb", False) is True:
            return
        if transport.is_closed():
            return
        if self._transport_deque.maxlen == len(self._transport_deque):
            transport.grace_close()
        else:
            setattr(transport, "is_absorb", True)
            self._transport_deque.append(transport)
            ping_future: asyncio.Future = asyncio.create_task(self._ping_handle(transport))
            transport.listen_future.add_done_callback(lambda _: safe_del_future(ping_future))
            transport.listen_future.add_done_callback(
                lambda _: self._transport_deque.remove(transport) if transport.available else None
            )

    async def fork_transport(self) -> Transport:
        """Create a new transport"""
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
        logger.debug("create transport:%s", transport.connection_info)
        return transport

    async def new_transport(self) -> Transport:
        transport: Transport = await self.fork_transport()
        self.absorption(transport)
        return transport

    @property
    def transport(self) -> Optional[Transport]:
        self._transport_deque.rotate(1)
        while True:
            if not self._transport_deque:
                return None
            transport: Transport = self._transport_deque[0]
            if not transport.available:
                # pop transport and close
                self._transport_deque.popleft().grace_close()
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
        await self.new_transport()
        # Number of links maintained in the back office
        self._transport_manager_future = asyncio.create_task(self._transport_manger())

    async def destroy(self) -> None:
        self._expected_number_of_transports = 0
        self._transport_manager_event.set()
        await self._transport_manager_future

    def __len__(self) -> int:
        return len(self._transport_deque)
