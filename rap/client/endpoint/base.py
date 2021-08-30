import asyncio
import logging
import random
from enum import Enum, auto
from typing import Any, Dict, List, Optional, Set

from rap.client.transport.transport import Transport
from rap.common.conn import Connection


class PickConnEnum(Enum):
    random = auto()
    round_robin = auto()
    faster = auto()


class Picker(object):
    def __init__(self, conn: Connection):
        self._conn: Connection = conn

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
        ping_sleep_time: Optional[int] = None,
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
        """
        self._transport: Optional[Transport] = None
        self._host_weight_list: List[str] = []
        self._timeout: int = timeout or 1200
        self._ssl_crt_path: Optional[str] = ssl_crt_path
        self._pack_param: Optional[dict] = pack_param
        self._unpack_param: Optional[dict] = unpack_param

        self._ping_sleep_time: Optional[int] = ping_sleep_time
        self._ping_fail_cnt: Optional[int] = ping_fail_cnt
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
        assert isinstance(transport, Transport), TypeError(f"{transport} type must{Transport}")
        self._transport = transport

    @property
    def is_close(self) -> bool:
        return self._is_close

    async def create(self, ip: str, port: int, weight: int = 10) -> None:
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
        )

        def _conn_done(f: asyncio.Future) -> None:
            try:
                if key in self._conn_dict:
                    del self._conn_dict[key]
                    self._host_weight_list.remove(key)
                self._connected_cnt -= 1
            except Exception as e:
                logging.exception(f"close conn error: {e}")

        await conn.connect()
        await self._transport.declare(conn)
        self._connected_cnt += 1
        conn.listen_future = asyncio.ensure_future(self._transport.listen(conn))
        conn.listen_future.add_done_callback(lambda f: _conn_done(f))
        conn.ping_future = asyncio.ensure_future(
            self._transport.ping_event(
                conn,
                ping_sleep_time=self._ping_sleep_time,
                ping_fail_cnt=self._ping_fail_cnt,
                wait_server_recover=self._wait_server_recover,
            )
        )
        if not self._wait_server_recover:
            conn.ping_future.add_done_callback(lambda f: conn.close())
        logging.debug("Connection to %s...", conn.connection_info)

        self._host_weight_list.extend([key for _ in range(weight)])
        random.shuffle(self._host_weight_list)

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

        self._host_weight_list = [i for i in self._host_weight_list if i != key]
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
        self._host_weight_list = []
        self._conn_dict = {}
        self._is_close = True

    def picker(self, cnt: Optional[int] = None) -> Picker:
        """get conn by endpoint
        cnt: How many conn to get.
          if the value is empty, it will automatically get 1/3 of the conn from the endpoint,
          which should not be less than or equal to 0
        """
        if not cnt:
            cnt = self._connected_cnt // 3
        if cnt <= 0:
            cnt = 1

        return Picker(sorted(self._pick_conn(cnt), key=lambda c: c.priority)[0])

    def _pick_conn(self, cnt: int) -> List[Connection]:
        pass

    def _random_pick_conn(self, cnt: int) -> List[Connection]:
        """random get conn"""
        if not self._host_weight_list:
            raise ConnectionError("Endpoint Can not found conn")

        cnt = min(cnt, len(self._conn_dict))
        conn_set: Set[Connection] = set()
        while len(conn_set) < cnt:
            key: str = random.choice(self._host_weight_list)
            conn_set.add(self._conn_dict[key])

        return list(conn_set)

    def _round_robin_pick_conn(self, cnt: int) -> List[Connection]:
        """get conn by round robin"""
        if not self._host_weight_list:
            raise ConnectionError("Endpoint Can not found conn")

        conn_list: List[Connection] = []
        for _ in range(cnt):
            self._round_robin_index += 1
            index = self._round_robin_index % (len(self._host_weight_list))
            key = self._host_weight_list[index]
            conn_list.append(self._conn_dict[key])
        return conn_list

    def _pick_faster_conn(self, cnt: int) -> List[Connection]:
        return sorted([i for i in self._conn_dict.values()], key=lambda c: c.RTT)[:cnt]

    def _best_available_pick(self) -> List[Connection]:
        pass

    def __len__(self) -> int:
        return self._connected_cnt
