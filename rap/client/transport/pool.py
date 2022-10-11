import asyncio
import logging
import random
import time
from typing import TYPE_CHECKING, List, Optional

from rap.client.transport.transport import Transport
from rap.common.asyncio_helper import (
    Deadline,
    IgnoreDeadlineTimeoutExc,
    SetEvent,
    Share,
    done_future,
    get_event_loop,
    safe_del_future,
)
from rap.common.number_range import get_value_by_range

logger: logging.Logger = logging.getLogger(__name__)
if TYPE_CHECKING:
    from rap.client.core import BaseClient


class PoolWrapTransport(object):
    """Wrap Transport for Pool calls"""

    def __init__(
        self,
        transport: Transport,
        transport_age: Optional[int] = None,
        transport_age_jitter: Optional[int] = None,
        min_ping_interval: Optional[int] = None,
        max_ping_interval: Optional[int] = None,
        ping_fail_cnt: Optional[int] = None,
    ):
        self.transport: Transport = transport
        # ping
        self._min_ping_interval: int = min_ping_interval or 1
        self._max_ping_interval: int = max_ping_interval or 3
        self._ping_fail_cnt: int = ping_fail_cnt or 3
        ping_future: asyncio.Future = asyncio.ensure_future(self._ping_handle())
        self.transport.add_close_callback(lambda _: safe_del_future(ping_future))
        # age
        if transport_age and not transport_age_jitter:
            raise ValueError("transport_age_jitter must be set if transport_age is set")
        if transport_age and transport_age_jitter:
            transport_age_time_handle: asyncio.TimerHandle = get_event_loop().call_later(
                int(transport_age + random.randint(0, transport_age_jitter)), self.transport.grace_close
            )
            self.transport.add_close_callback(lambda _: transport_age_time_handle.cancel())

    @property
    def pick_score(self) -> float:
        return self.transport.pick_score

    @property
    def inflight(self) -> int:
        return self.transport.inflight

    @property
    def is_active(self) -> bool:
        return self.transport.available and not self.transport.is_closed()

    def grace_close(self) -> None:
        self.transport.grace_close()

    async def _ping_handle(self) -> None:
        ping_fail_interval: int = int(self._max_ping_interval * self._ping_fail_cnt)

        try:
            while True:
                # If no event loop is running, should just exit
                get_event_loop()

                if not self.is_active:
                    return
                now_time: float = time.time()
                available: bool = (now_time - self.transport.last_ping_timestamp) < ping_fail_interval
                logger.debug("transport:%s available:%s", self.transport.connection_info, available)
                if not available:
                    logger.error(f"conn:{self.transport.conn_id} ping {self.transport.sock_tuple} timeout, exit")
                    return
                try:
                    with Deadline(
                        random.randint(self._min_ping_interval, self._max_ping_interval),
                        timeout_exc=IgnoreDeadlineTimeoutExc(),
                    ) as d:
                        await self.transport.ping()
                        await self.transport.sleep_and_listen(d.surplus)
                except asyncio.CancelledError:
                    return
                except Exception as e:
                    logger.debug(f"{self.transport.connection_info} ping event error:{e}")
        finally:
            # Arrange for a graceful shutdown of the transport
            self.transport.grace_close()


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
        transport_grace_timeout: Optional[int] = None,
        pool_high_water: Optional[float] = None,
        pool_lower_water: Optional[float] = None,
        destroy_transport_interval: Optional[int] = None,
    ) -> None:
        """
        :param app: rap client
        :param host: server host
        :param port: server port

        :param ssl_crt_path: set conn ssl_crt_path
        :param pack_param: set conn pack param
        :param unpack_param: set conn unpack param
        :param read_timeout: set conn read timeout param
        :param max_inflight: set conn max use number, default 100

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
        :param destroy_transport_interval:
            When dynamically adjusting the transport, the interval for continuously destroying the transport.
             default value 60 (second)

             Creating transport is very expensive, so the pool will slowly destroy the transport,
             preventing frequent destruction and creation of transport in a short period of time.
        """
        self._app: "BaseClient" = app
        self._share: Share = Share()
        self._host: str = host
        self._port: int = port
        self._weight: int = weight
        self._ssl_crt_path: Optional[str] = ssl_crt_path
        self._pack_param: Optional[dict] = pack_param
        self._unpack_param: Optional[dict] = unpack_param
        self._max_inflight: int = max_inflight or 100
        self._read_timeout: Optional[int] = read_timeout
        self._declare_timeout: int = declare_timeout or 9
        self._min_ping_interval: Optional[int] = min_ping_interval
        self._max_ping_interval: Optional[int] = max_ping_interval
        self._ping_fail_cnt: Optional[int] = ping_fail_cnt
        self._max_pool_size: int = max_pool_size or 3
        self._min_pool_size: int = min_pool_size or 1
        self._transport_age: Optional[int] = transport_age
        self._transport_age_jitter: Optional[int] = transport_age_jitter
        self._transport_grace_timeout: Optional[int] = transport_grace_timeout
        self._pool_high_water: float = get_value_by_range(pool_high_water or 0.8, 0, 1)
        self._pool_lower_water: float = get_value_by_range(pool_lower_water or 0.2, 0, 1)

        self._active_transport_list: List[PoolWrapTransport] = []

        self._transport_manager_future: asyncio.Future = done_future()
        self._transport_manager_event: asyncio.Event = asyncio.Event()
        self._expected_number_of_transports: int = 0
        self._transport_index: int = 0

        self._pick_score: float = 0.0
        self._destroy_transport_timestamp: float = time.time()
        self._destroy_transport_interval: int = destroy_transport_interval or 60

    @property
    def is_close(self):
        """Whether the pool is closed"""
        return self._transport_manager_future.done()

    async def _transport_manger(self) -> None:
        """Ensure that the number of transports is adjusted to the desired value during the Pool run"""
        self._transport_manager_event.set()
        add_transport_set_event: SetEvent[asyncio.Future] = SetEvent()
        while True:
            # Make sure can run it every once in a while and clear out unavailable transports in time
            await asyncio.wait([self._transport_manager_event.wait()], timeout=1)
            transport_len: int = len(self._active_transport_list)
            if not transport_len:
                self._pick_score = 0
            else:
                self._pick_score = sum([i.pick_score for i in self._active_transport_list]) / transport_len

                # Reordered to facilitate subsequent cleanup of transport
                self._active_transport_list.sort(key=lambda x: x.inflight)
                # Reset the traversed cursor so that the top-ranked transport will be obtained first
                self._transport_index = 0
                # Clear not active transport
                for index in range(len(self._active_transport_list)):
                    if not self._active_transport_list:
                        break
                    if self._active_transport_list[-1].is_active:
                        break

                    try:
                        self._active_transport_list.pop().grace_close()
                    except Exception as e:
                        logger.warning(f"ignore transport manger close {self._host}:{self._port} error:{e}")

                # Prevent execution for too long and affect the operation of other coroutines
                await asyncio.sleep(0)

            transport_len: int = len(self._active_transport_list)
            self._expected_number_of_transports = get_value_by_range(
                self._expected_number_of_transports, 0, self._max_pool_size
            )

            if transport_len > self._expected_number_of_transports:
                # Remove redundant transports (with lower scores)
                for wrap_transport in self._active_transport_list[self._expected_number_of_transports :]:
                    try:
                        wrap_transport.transport.grace_close()
                    except Exception as e:
                        logger.warning(f"ignore transport manger close {self._host}:{self._port} error:{e}")
                self._active_transport_list = self._active_transport_list[: self._expected_number_of_transports]
                # Prevent execution for too long and affect the operation of other coroutines
                await asyncio.sleep(0)

            if transport_len < self._expected_number_of_transports and not add_transport_set_event:
                for _ in range(self._expected_number_of_transports - transport_len):
                    add_transport_future: asyncio.Future = asyncio.ensure_future(self.add_transport())
                    # TODO If exceptions are always created, this logic will be executed all the time and will not sleep
                    add_transport_set_event.add(add_transport_future)
                    add_transport_future.add_done_callback(lambda f: add_transport_set_event.remove(f))

            if transport_len == 0 and self._expected_number_of_transports == 0:
                break
            elif transport_len == self._expected_number_of_transports:
                # If the number of transports is the same as the desired number, wait notify
                self._transport_manager_event.clear()

        if add_transport_set_event:
            for pending_future in add_transport_set_event:
                safe_del_future(pending_future)

    def _incr(self) -> None:
        """Notify the pool to incr transport as soon as possible"""
        self._expected_number_of_transports += 1
        self._transport_manager_event.set()

    def _decr(self) -> None:
        """Notify the pool to decr transport,but the pool will not process it right away"""
        if time.time() - self._destroy_transport_timestamp < self._destroy_transport_interval:
            # Reduce the speed of shrinking
            return
        if self._expected_number_of_transports > 1 and len(self._active_transport_list) > 1:
            self._expected_number_of_transports -= 1
            self._transport_manager_event.set()
            self._destroy_transport_timestamp = time.time()

    @property
    def pick_score(self) -> float:
        return self._pick_score

    def absorption(self, transport: Transport) -> Optional[PoolWrapTransport]:
        """Absorb the transport into the pool."""
        if transport.is_closed() or not transport.available:
            return
        wrap_transport: PoolWrapTransport = PoolWrapTransport(
            transport,
            transport_age=self._transport_age,
            transport_age_jitter=self._transport_age_jitter,
            min_ping_interval=self._min_ping_interval,
            max_ping_interval=self._max_ping_interval,
            ping_fail_cnt=self._ping_fail_cnt,
        )
        self._active_transport_list.append(wrap_transport)
        return wrap_transport

    async def fork_transport(self) -> Transport:
        """Create new transport. (The transport created at this time will not be placed in the pool)"""
        transport: Transport = Transport(
            self._app,
            self._host,
            self._port,
            self._weight,
            ssl_crt_path=self._ssl_crt_path,
            pack_param=self._pack_param,
            unpack_param=self._unpack_param,
            max_inflight=self._max_inflight,
            read_timeout=self._read_timeout,
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
        return transport

    async def add_transport(self) -> PoolWrapTransport:
        """Create new transport and place it in the pool"""
        if self._expected_number_of_transports == 0:
            raise RuntimeError("Pool is close, can not create new transport")
        if len(self._active_transport_list) >= self._max_pool_size:
            raise ValueError("Transport pool is full and cannot be added further")

        @self._share.wrapper_do(key=str(id(self)))
        async def _add_transport() -> PoolWrapTransport:
            wrap_transport: Optional[PoolWrapTransport] = self.absorption(await self.fork_transport())
            if wrap_transport is None:
                raise ConnectionError("Create transport fail")
            return wrap_transport

        return await _add_transport()

    def _change_by_transport(self, wrap_transport: PoolWrapTransport) -> None:
        if wrap_transport.inflight <= self._max_inflight * self._pool_lower_water:
            self._decr()
        elif wrap_transport.inflight >= self._max_inflight * self._pool_high_water:
            self._incr()

    def unuse_transport(self, wrap_transport: PoolWrapTransport) -> None:
        if not wrap_transport.is_active:
            self._transport_manager_event.set()
        self._change_by_transport(wrap_transport)

    async def use_transport(self) -> PoolWrapTransport:
        wrap_transport: Optional[PoolWrapTransport] = None
        while True:
            if not self._active_transport_list:
                break
            wrap_transport = self._active_transport_list[self._transport_index % len(self._active_transport_list)]
            self._transport_index += 1
            if not wrap_transport.is_active:
                self._transport_manager_event.set()
            else:
                break
        if wrap_transport is None:
            self._incr()
            wrap_transport = await self.add_transport()
        self._change_by_transport(wrap_transport)
        return wrap_transport

    async def create(self) -> None:
        """
        Initialize the transport pool,
        normally the number of generated transport is less than or equal to min pool size
        """
        if not self.is_close:
            return
        transport_group_len: int = len(self._active_transport_list)
        if transport_group_len >= self._max_pool_size:
            return
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
