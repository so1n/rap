import asyncio
import logging
import random
import time
from collections import deque
from typing import TYPE_CHECKING, Deque, Optional

from rap.client.transport.transport import Transport
from rap.common.asyncio_helper import Deadline, IgnoreDeadlineTimeoutExc, done_future, safe_del_future

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
        self._create_transport_future: asyncio.Future = done_future()

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
                transport.inflight_load.append(transport.semaphore.inflight)
                # Simple design, don't want to use pandas&numpy in the app framework
                avg_inflight: float = sum(transport.inflight_load) / len(transport.inflight_load)
                if avg_inflight > 80 and len(self) < self._max_pool_size:
                    # The current transport is under too much pressure and
                    # transport needs to be created to divert the traffic
                    if self._create_transport_future.done():
                        # Prevent multiple transport's ping handler from creating new transport at the same time
                        self._create_transport_future = asyncio.create_task(self.create_new())
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
            safe_del_future(self._create_transport_future)
            transport.close()

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
            else:
                return transport

    async def create_new(self) -> None:
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
                await transport.await_close()
            raise e
        ping_future: asyncio.Future = asyncio.create_task(self._ping_handle(transport))
        transport.listen_future.add_done_callback(lambda _: safe_del_future(ping_future))
        self._transport_deque.append(transport)

    async def destroy(self) -> None:
        while self._transport_deque:
            await self._transport_deque.pop().await_close()

    def __len__(self) -> int:
        return len(self._transport_deque)
