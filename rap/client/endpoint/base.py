import asyncio
import logging
import random
import time
from collections import deque
from enum import Enum, auto
from typing import Any, Deque, Dict, List, Optional, Tuple

from rap.client.transport.transport import Transport
from rap.common.asyncio_helper import Deadline, IgnoreDeadlineTimeoutExc
from rap.common.conn import Connection

logger: logging.Logger = logging.getLogger(__name__)


class BalanceEnum(Enum):
    """Balance method
    random: random pick a conn
    round_robin: round pick conn
    """

    random = auto()
    round_robin = auto()


class ConnGroup(object):
    def __init__(self) -> None:
        self._conn_deque: Deque[Connection] = deque()

    @property
    def conn(self) -> Connection:
        conn: Connection = self._conn_deque[0]
        self._conn_deque.rotate(1)
        return conn

    def add(self, conn: Connection) -> None:
        self._conn_deque.append(conn)

    def remove(self, conn: Connection) -> None:
        self._conn_deque.remove(conn)

    async def destroy(self) -> None:
        while self._conn_deque:
            await self._conn_deque.pop().await_close()

    def __len__(self) -> int:
        return len(self._conn_deque)


class Picker(object):
    """auto pick conn, refer to `Kratos` 1.x"""

    def __init__(self, conn_list: List[Connection]):
        self._conn: Connection = self._pick(conn_list)
        self._start_time: float = time.time()

    @staticmethod
    def _pick(conn_list: List[Connection]) -> Connection:
        """pick by score"""
        pick_conn: Optional[Connection] = None
        conn_len: int = len(conn_list)
        if conn_len == 1:
            return conn_list[0]
        elif conn_len > 1:
            score: float = 0.0
            for conn in conn_list:
                conn_inflight: float = conn.semaphore.inflight
                _score: float = conn.score
                if conn_inflight:
                    _score = _score * (1 - conn_inflight / conn.semaphore.raw_value)
                logger.debug(
                    "conn:%s available:%s available_level:%s rtt:%s score:%s",
                    conn.peer_tuple,
                    conn.available,
                    conn.available_level,
                    conn.rtt,
                    _score,
                )
                if _score > score:
                    score = _score
                    pick_conn = conn
        if not pick_conn:
            raise ValueError("Can not found available conn")
        return pick_conn

    async def __aenter__(self) -> Connection:
        await self._conn.semaphore.acquire()
        return self._conn

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self._conn.semaphore.release()
        return None


class BaseEndpoint(object):
    def __init__(
        self,
        transport: Transport,
        declare_timeout: Optional[int] = None,
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
        :param transport: client transport
        :param declare_timeout: declare timeout include request & response, default 9
        :param ssl_crt_path: client ssl crt file path
        :param balance_enum: balance pick conn method, default random
        :param pack_param: msgpack pack param
        :param unpack_param: msgpack unpack param
        :param min_ping_interval: send client ping min interval, default 1
        :param max_ping_interval: send client ping max interval, default 3
        :param ping_fail_cnt: How many times ping fails to judge as unavailable, default 3
        """
        self._transport: Transport = transport
        self._declare_timeout: int = declare_timeout or 9
        self._ssl_crt_path: Optional[str] = ssl_crt_path
        self._pack_param: Optional[dict] = pack_param
        self._unpack_param: Optional[dict] = unpack_param

        self._min_ping_interval: int = min_ping_interval or 1
        self._max_ping_interval: int = max_ping_interval or 3
        self._ping_fail_cnt: int = ping_fail_cnt or 3
        self._max_pool_size: int = max_pool_size or 3
        self._min_pool_size: int = min_poll_size or 1

        self._connected_cnt: int = 0
        self._conn_key_list: List[Tuple[str, int]] = []
        self._conn_group_dict: Dict[Tuple[str, int], ConnGroup] = {}
        self._round_robin_index: int = 0
        self._is_close: bool = True

        setattr(self, self._pick_conn.__name__, self._random_pick_conn)
        if balance_enum:
            if balance_enum == BalanceEnum.random:
                setattr(self, self._pick_conn.__name__, self._random_pick_conn)
            elif balance_enum == BalanceEnum.round_robin:
                setattr(self, self._pick_conn.__name__, self._round_robin_pick_conn)

    async def _listen_conn(self, conn: Connection) -> None:
        """listen server msg from conn"""
        logger.debug("listen:%s start", conn.peer_tuple)
        try:
            while not conn.is_closed():
                await self._transport.response_handler(conn)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            conn.set_reader_exc(e)
            logger.exception(f"listen {conn.connection_info} error:{e}")
            if not conn.is_closed():
                await conn.await_close()

    async def _ping_event(self, conn: Connection) -> None:
        """client ping-pong handler, check conn is available"""
        ping_fail_interval: int = int(self._max_ping_interval * self._ping_fail_cnt)
        while True:
            now_time: float = time.time()
            diff_time: float = now_time - conn.last_ping_timestamp
            available: bool = diff_time < ping_fail_interval
            logger.debug("conn:%s available:%s rtt:%s", conn.peer_tuple, available, conn.rtt)
            if not available:
                logger.error(f"ping {conn.sock_tuple} timeout... exit")
                return
            elif not (self._min_ping_interval == 1 and self._max_ping_interval == 1):
                conn.inflight_load.append(conn.semaphore.inflight)
                # Simple design, don't want to use pandas&numpy in the web framework
                conn_group: ConnGroup = self._conn_group_dict[(conn.host, conn.port)]
                avg_inflight: float = sum(conn.inflight_load) / len(conn.inflight_load)
                if avg_inflight > 80 and len(conn_group) < self._max_pool_size:
                    await self.create(conn.host, conn.port, conn.weight, conn.semaphore.raw_value)
                elif avg_inflight < 20 and len(conn_group) > self._min_pool_size:
                    # When conn is just created, inflight is 0
                    conn.available_level -= 1
                elif conn.available and conn.available_level < 5:
                    conn.available_level += 1

            if conn.available_level <= 0 and conn.available:
                conn.close_soon()

            logger.debug("conn:%s available:%s rtt:%s", conn.peer_tuple, conn.available_level, conn.rtt)
            next_ping_interval: int = random.randint(self._min_ping_interval, self._max_ping_interval)
            try:
                with Deadline(next_ping_interval, timeout_exc=IgnoreDeadlineTimeoutExc()) as d:
                    await self._transport.ping(conn)
                    await conn.sleep_and_listen(d.surplus)
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.debug(f"{conn} ping event error:{e}")

    @property
    def is_close(self) -> bool:
        return self._is_close

    async def create(
        self,
        ip: str,
        port: int,
        weight: Optional[int] = None,
        max_conn_inflight: Optional[int] = None,
    ) -> None:
        """create and init conn
        :param ip: server ip
        :param port: server port
        :param weight: select conn weight
        :param max_conn_inflight: Maximum number of connections per conn
        """
        if not weight:
            weight = 10
        if not max_conn_inflight:
            max_conn_inflight = 100

        key: Tuple[str, int] = (ip, port)
        if key not in self._conn_group_dict:
            self._conn_group_dict[key] = ConnGroup()
            self._conn_key_list.append(key)

        if len(self._conn_group_dict[key]) >= self._max_pool_size:
            return
        create_size: int = 1
        if not self._conn_group_dict[key]:
            create_size = self._min_pool_size
        for _ in range(create_size):
            conn: Connection = Connection(
                ip,
                port,
                weight,
                ssl_crt_path=self._ssl_crt_path,
                pack_param=self._pack_param,
                unpack_param=self._unpack_param,
                max_conn_inflight=max_conn_inflight,
            )

            def _conn_done(f: asyncio.Future) -> None:
                try:
                    try:
                        conn_group: Optional[ConnGroup] = self._conn_group_dict.get(key, None)
                        if conn_group:
                            conn_group.remove(conn)
                            if not conn_group:
                                self._conn_group_dict.pop(key)
                                self._conn_key_list.remove(key)
                    except ValueError:
                        pass
                    self._connected_cnt -= 1
                except Exception as _e:
                    msg: str = f"close conn error: {_e}"
                    if f.exception():
                        msg += f", conn done exc:{f.exception()}"
                    logger.exception(msg)

            try:
                with Deadline(self._declare_timeout, timeout_exc=asyncio.TimeoutError(f"conn:{conn} declare timeout")):
                    await conn.connect()
                    self._connected_cnt += 1
                    conn.listen_future = asyncio.ensure_future(self._listen_conn(conn))
                    conn.listen_future.add_done_callback(lambda f: _conn_done(f))
                    await self._transport.declare(conn)
            except Exception as e:
                await conn.await_close()
                raise e
            self._conn_group_dict[key].add(conn)
            conn.ping_future = asyncio.ensure_future(self._ping_event(conn))
            conn.ping_future.add_done_callback(lambda f: conn.close())

    @staticmethod
    async def destroy(conn_group: ConnGroup) -> None:
        await conn_group.destroy()

    async def _start(self) -> None:
        self._is_close = False

    async def start(self) -> None:
        """start endpoint and create&init conn"""
        raise NotImplementedError

    async def stop(self) -> None:
        """stop endpoint and close all conn and cancel future"""
        while self._conn_key_list:
            await self.destroy(self._conn_group_dict[self._conn_key_list.pop()])

        assert not self._conn_key_list
        assert not self._conn_group_dict
        self._is_close = True

    def picker(self, cnt: int = 3) -> Picker:
        """get conn by endpoint
        :param cnt: How many conn to get.
        """
        if not self._conn_key_list:
            raise ConnectionError("Endpoint Can not found available conn")
        cnt = min(self._connected_cnt, cnt)
        conn_list: List[Connection] = self._pick_conn(cnt)
        return Picker([conn for conn in conn_list if conn.available])

    def _pick_conn(self, cnt: int) -> List[Connection]:
        pass

    def _random_pick_conn(self, cnt: int) -> List[Connection]:
        """random get conn"""
        key_list: List[Tuple[str, int]] = random.choices(self._conn_key_list, k=cnt)
        return [self._conn_group_dict[key].conn for key in key_list]

    def _round_robin_pick_conn(self, cnt: int) -> List[Connection]:
        """get conn by round robin"""
        self._round_robin_index += 1
        index: int = self._round_robin_index % (len(self._conn_key_list))
        key_list: List[Tuple[str, int]] = self._conn_key_list[index : index + cnt]
        return [self._conn_group_dict[key].conn for key in key_list]

    def __len__(self) -> int:
        return self._connected_cnt
