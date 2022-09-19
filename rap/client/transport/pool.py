import asyncio
import logging
import random
import time
from collections import deque
from typing import TYPE_CHECKING, Deque, List, Optional

from rap.client.transport.transport import Transport
from rap.common.asyncio_helper import Deadline, done_future
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
        """
        :param app: rap client
        :param host: server host
        :param port: server port

        :param ssl_crt_path: set conn ssl_crt_path
        :param pack_param: set conn pack param
        :param unpack_param: set conn unpack param
        :param read_timeout: set conn read timeout param
        :param max_inflight: set conn max use number

        :param weight: set transport weight
        :param declare_timeout: set transport declare timeout

        :param min_ping_interval: Minimum interval time (seconds)
        :param max_ping_interval: Maximum interval time (seconds)
        :param ping_fail_cnt: The maximum number of consecutive ping failures.
            If the number of consecutive failures is greater than or equal to the value, conn will be closed
        :param max_pool_size: Maximum number of conn
        :param min_pool_size: Minimum number of conn
        :param transport_age: Set the number of seconds after which the transport will be deactivated
            (no new requests are allowed)，If it is 0, the function is not enabled
        :param transport_age_jitter: Prevent transport from being closed in batches by jitter.
            When the transport age is not empty, the transport age jitter cannot be empty either.
        :param transport_max_age: Set the number of seconds after the stop is used before closing the transport to
            prevent affecting the request being processed (default 3600 seconds)
        """
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
        self._min_ping_interval: Optional[int] = min_ping_interval
        self._max_ping_interval: Optional[int] = max_ping_interval
        self._ping_fail_cnt: Optional[int] = ping_fail_cnt
        self._max_pool_size: int = max_pool_size or 3
        self._min_pool_size: int = min_pool_size or 1

        if transport_age and not transport_age_jitter:
            raise ValueError("transport_age_jitter must be set if transport_age is set")
        self._transport_max_age: int = transport_max_age or ((transport_age or 0) + 3600)
        if transport_age and transport_age_jitter:
            self.transport_end_time: float = time.time() + int(transport_age + random.randint(0, transport_age_jitter))
        else:
            self.transport_end_time = 0

        self._transport_deque: Deque[Transport] = deque()
        self._transport_list: List[Transport] = []
        self._transport_manager_future: asyncio.Future = done_future()
        self._transport_manager_event: asyncio.Event = asyncio.Event()
        self._expected_number_of_transports: int = 0
        self._lock: asyncio.Lock = asyncio.Lock()

    @property
    def is_close(self):
        """Whether the pool is closed"""
        return self._transport_manager_future.done()

    async def _transport_manger(self) -> None:
        """Ensure that the number of transports is adjusted to the desired value during the Pool run"""
        self._transport_manager_event.set()
        while True:
            # Make sure you can run it every once in a while and clear out unavailable transports in time
            await asyncio.wait([self._transport_manager_event.wait()], timeout=60)

            for _ in range(len(self._transport_deque)):
                self._transport_deque.rotate(1)
                if not self._transport_deque[0].available:
                    try:
                        self._transport_deque.popleft().grace_close()
                    except Exception as e:
                        logger.warning(f"ignore transport manger close {self._host}:{self._port} error:{e}")
                    break

            transport_len: int = len(self._transport_deque)
            self._expected_number_of_transports = get_value_by_range(
                self._expected_number_of_transports, 0, self._max_pool_size
            )

            if transport_len > self._expected_number_of_transports:
                for _ in range(transport_len - self._expected_number_of_transports):
                    try:
                        self._transport_deque.popleft().grace_close()
                    except Exception as e:
                        logger.warning(f"ignore transport manger close {self._host}:{self._port} error:{e}")

            if transport_len < self._expected_number_of_transports:
                for _ in range(self._expected_number_of_transports - transport_len):
                    try:
                        await self.add_transport()
                    except Exception as e:
                        logger.warning(f"ignore transport manger create {self._host}:{self._port} error:{e}")

            if transport_len == 0 and self._expected_number_of_transports == 0:
                return
            elif transport_len == self._expected_number_of_transports:
                # If the number of transports is the same as the desired number, wait notify
                self._transport_manager_event.clear()

    def _ping_callback(self, transport: Transport) -> None:
        now_time: float = time.time()
        if self.transport_end_time and now_time > self.transport_end_time:
            if transport.available:
                # When the transport time exceeds the limit, need to set the transport to become unavailable,
                # so that it will not accept new requests, and wait for the pool manager to reorganize the transport
                transport.grace_close()
                self._transport_manager_event.set()
            else:
                # If the transport exceeds the specified maximum survival time,
                # the transport will be closed immediately
                if now_time > self.transport_end_time + self._transport_max_age:
                    transport.close()
            return
        median_inflight: int = sorted(transport.inflight_load)[len(transport.inflight_load) // 2 + 1]
        if median_inflight > 80 and len(self) < self._max_pool_size:
            # The current transport is under too much pressure and
            # transport needs to be created to divert the traffic
            self._expected_number_of_transports += 1
            self._transport_manager_event.set()

    def absorption(self, transport: Transport) -> None:
        """Absorb the transport into the pool."""
        if getattr(transport, "_is_absorb", False) is True:
            return
        if transport.is_closed():
            return
        if self._transport_deque.maxlen == len(self._transport_deque):
            transport.grace_close()
        else:
            setattr(transport, "_is_absorb", True)
            self._transport_deque.append(transport)
            transport.add_ping_callback(self._ping_callback)
            transport.add_close_callback(
                lambda _: self._transport_deque.remove(transport) if transport.available else None
            )

    async def fork_transport(self) -> Transport:
        """Create a new transport. (The transport created at this time will not be placed in the pool)"""
        transport: Transport = Transport(
            self._app,
            self._host,
            self._port,
            self._weight,
            ssl_crt_path=self._ssl_crt_path,
            pack_param=self._pack_param,
            unpack_param=self._unpack_param,
            max_inflight=self._max_inflight,
            ping_fail_cnt=self._ping_fail_cnt,
            max_ping_interval=self._max_ping_interval,
            min_ping_interval=self._min_ping_interval,
        )

        try:
            with Deadline(self._declare_timeout, timeout_exc=asyncio.TimeoutError("transport declare timeout")):
                await transport.connect()
                await transport.declare()
        except Exception as e:
            if not transport.is_closed():
                try:
                    await transport.await_close()
                except Exception as close_e:
                    logger.error(f"ignore {transport.connection_info} close error:{close_e}")
            raise e
        logger.debug("create transport:%s", transport.connection_info)
        return transport

    async def add_transport(self) -> Transport:
        """Create a new transport and place it in the pool"""
        if self._transport_deque.maxlen == len(self._transport_deque):
            raise ValueError("Transport pool is full and cannot be added further")

        transport: Transport = await self.fork_transport()
        self.absorption(transport)
        return transport

    async def get_transport(self) -> Transport:
        """Guaranteed to get transport"""
        async with self._lock:
            transport: Optional[Transport] = self.transport
            if not transport:
                transport = await self.add_transport()
            return transport

    @property
    def transport(self) -> Optional[Transport]:
        """try to get transport from pool"""
        self._transport_deque.rotate(1)  # Rotate before each call(O(1))
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
        await self.add_transport()
        # Number of links maintained in the back office
        self._transport_manager_future = asyncio.create_task(self._transport_manger())

    async def destroy(self) -> None:
        """close pool
        Will first set the expected value to 0, and wait for the transport to be cleaned up
        """
        self._expected_number_of_transports = 0
        self._transport_manager_event.set()
        await self._transport_manager_future

    def __len__(self) -> int:
        return len(self._transport_deque)
