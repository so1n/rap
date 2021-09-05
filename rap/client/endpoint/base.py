import asyncio
import logging
import random
import time
from enum import Enum, auto
from typing import Any, Dict, List, Optional, Set

from rap.client.transport.transport import Transport
from rap.common.conn import Connection


class PickConnEnum(Enum):
    random = auto()
    round_robin = auto()
    faster = auto()


class Picker(object):
    """refer to `Kratos`"""

    def __init__(self, conn_list: List[Connection]):
        self._conn: Connection = self._pick(conn_list)
        self._start_time: float = time.time()

    @staticmethod
    def _pick(conn_list: List[Connection]) -> Connection:
        """pick by score"""
        pick_conn: Optional[Connection] = None
        conn_len: int = len(conn_list)
        if conn_len == 1:
            pick_conn = conn_list[0]
        elif conn_len > 1:
            score: float = 0.0
            for conn in conn_list:
                _score: float = conn.score / conn.semaphore.value
                logging.debug("conn:%s available:%s rtt:%s score:%s", conn.peer_tuple, conn.available, conn.rtt, _score)
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
        timeout: Optional[int] = None,
        ssl_crt_path: Optional[str] = None,
        pick_conn_method: Optional[PickConnEnum] = None,
        pack_param: Optional[dict] = None,
        unpack_param: Optional[dict] = None,
        min_ping_interval: Optional[int] = None,
        max_ping_interval: Optional[int] = None,
        ping_fail_cnt: Optional[int] = None,
        wait_server_recover: bool = True,
    ) -> None:
        """
        conn_list: client conn info
          include ip, port, weight
          ip: server ip
          port: server port
          weight: select this conn weight
          e.g.  [{"ip": "localhost", "port": "9000", weight: 10}]
        timeout: read response from consumer timeout
        :param min_ping_interval: send client ping min interval, default 1
        :param max_ping_interval: send client ping max interval, default 3
        :param ping_fail_cnt: How many times ping fails to judge as unavailable, default 3
        :param wait_server_recover: If False, ping failure will close conn
        """
        self._transport: Optional[Transport] = None
        self._timeout: int = timeout or 1200
        self._ssl_crt_path: Optional[str] = ssl_crt_path
        self._pack_param: Optional[dict] = pack_param
        self._unpack_param: Optional[dict] = unpack_param

        self._min_ping_interval: int = min_ping_interval or 1
        self._max_ping_interval: int = max_ping_interval or 3
        self._ping_fail_cnt: int = ping_fail_cnt or 3
        self._wait_server_recover: bool = wait_server_recover

        self._connected_cnt: int = 0
        self._conn_dict: Dict[str, Connection] = {}
        self._round_robin_index: int = 0
        self._is_close: bool = True

        setattr(self, self._pick_conn.__name__, self._random_pick_conn)
        if pick_conn_method:
            if pick_conn_method == PickConnEnum.random:
                setattr(self, self._pick_conn.__name__, self._random_pick_conn)
            elif pick_conn_method == PickConnEnum.round_robin:
                setattr(self, self._pick_conn.__name__, self._round_robin_pick_conn)
            elif pick_conn_method == PickConnEnum.faster:
                setattr(self, self._pick_conn.__name__, self._pick_faster_conn)

    def set_transport(self, transport: Transport) -> None:
        """set transport to endpoint"""
        assert not self._transport, ValueError("transport already exists")
        assert isinstance(transport, Transport), TypeError(f"{transport} type must{Transport}")
        self._transport = transport

    @property
    def is_close(self) -> bool:
        return self._is_close

    async def create(self, ip: str, port: int, weight: int = 10, max_conn_inflight: int = 100) -> None:
        """create and init conn
        ip: server ip
        port: server port
        weight: select conn weight
        """
        if not self._transport:
            raise ConnectionError("endpoint need transport")

        key: str = f"{ip}:{port}"
        if key in self._conn_dict:
            raise ConnectionError(f"conn:{key} already create")

        conn: Connection = Connection(
            ip,
            port,
            self._timeout,
            weight,
            ssl_crt_path=self._ssl_crt_path,
            pack_param=self._pack_param,
            unpack_param=self._unpack_param,
            max_conn_inflight=max_conn_inflight,
        )

        def _conn_done(f: asyncio.Future) -> None:
            try:
                if key in self._conn_dict:
                    del self._conn_dict[key]
                self._connected_cnt -= 1
            except Exception as e:
                msg: str = f"close conn error: {e}"
                if f.exception():
                    msg += f", conn done exc:{f.exception()}"
                logging.exception(msg)

        await conn.connect()
        await self._transport.declare(conn)
        self._connected_cnt += 1
        conn.available = True
        conn.listen_future = asyncio.ensure_future(self._transport.listen(conn))
        conn.listen_future.add_done_callback(lambda f: _conn_done(f))
        conn.ping_future = asyncio.ensure_future(
            self._transport.ping_event(
                conn,
                min_ping_interval=self._min_ping_interval,
                max_ping_interval=self._max_ping_interval,
                ping_fail_cnt=self._ping_fail_cnt,
                wait_server_recover=self._wait_server_recover,
            )
        )
        if not self._wait_server_recover:
            conn.ping_future.add_done_callback(lambda f: conn.close())
        logging.debug("Connection to %s...", conn.connection_info)

        self._conn_dict[key] = conn

    async def destroy(self, ip: str, port: int) -> None:
        """destroy conn
        ip: server ip
        port: server port
        """
        key: str = f"{ip}:{port}"
        if key not in self._conn_dict:
            return

        if not self.is_close:
            await self._conn_dict[key].await_close()

        if key in self._conn_dict:
            del self._conn_dict[key]

    async def start(self) -> None:
        """start endpoint and create&init conn"""
        self._is_close = False

    async def stop(self) -> None:
        """stop endpoint and close all conn and cancel future"""
        for key, conn in self._conn_dict.copy().items():
            if not conn.is_closed():
                await conn.await_close()
        self._conn_dict = {}
        self._is_close = True

    def picker(self, cnt: Optional[int] = None) -> Picker:
        """get conn by endpoint
        :param cnt: How many conn to get.
          if the value is empty, it will automatically get 1/3 of the conn from the endpoint,
          which should not be less than or equal to 0
        """
        if not cnt:
            if self._connected_cnt <= 3:
                cnt = self._connected_cnt
            else:
                cnt = self._connected_cnt // 3
        if cnt <= 0:
            cnt = 1

        conn_list: List[Connection] = self._pick_conn(cnt)
        return Picker(conn_list)

    def _pick_conn(self, cnt: int) -> List[Connection]:
        pass

    def _random_pick_conn(self, cnt: int) -> List[Connection]:
        """random get conn"""
        cnt = min(cnt, len(self._conn_dict))
        key_list: List[str] = list(self._conn_dict.keys())
        if not key_list:
            raise ConnectionError("Endpoint Can not found conn")
        conn_set: Set[Connection] = set()
        while len(conn_set) < cnt:
            key: str = random.choice(key_list)
            conn_set.add(self._conn_dict[key])

        return list([i for i in conn_set if i.available])

    def _round_robin_pick_conn(self, cnt: int) -> List[Connection]:
        """get conn by round robin"""
        conn_list: List[Connection] = []
        key_list: List[str] = list(self._conn_dict.keys())
        if not key_list:
            raise ConnectionError("Endpoint Can not found conn")
        for _ in range(cnt):
            self._round_robin_index += 1
            index = self._round_robin_index % (len(self._conn_dict))
            conn: Connection = self._conn_dict[key_list[index]]
            if conn.available:
                conn_list.append(conn)
        return conn_list

    def _pick_faster_conn(self, cnt: int) -> List[Connection]:
        return sorted([i for i in self._conn_dict.values()], key=lambda c: c.rtt)[:cnt]

    def __len__(self) -> int:
        return self._connected_cnt
