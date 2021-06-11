import asyncio
import logging
import random
from enum import Enum, auto
from typing import Callable, Dict, List, Optional, Set

from rap.client.transport.transport import Transport
from rap.common.conn import Connection


class SelectConnEnum(Enum):
    random = auto()
    round_robin = auto()


class BaseEndpoint(object):
    def __init__(
        self,
        server_name: str,
        timeout: int = 9,
        ssl_crt_path: Optional[str] = None,
        select_conn_method: SelectConnEnum = SelectConnEnum.random,
    ) -> None:
        self.server_name: str = server_name
        self._transport: Optional[Transport] = None
        self._host_weight_list: List[str] = []
        self._timeout: int = timeout
        self._ssl_crt_path: Optional[str] = ssl_crt_path
        self._connected_cnt: int = 0
        self._conn_dict: Dict[str, Connection] = {}
        self._round_robin_index: int = 0
        self._select_conn_method: SelectConnEnum = select_conn_method

    def set_transport(self, transport: Transport) -> None:
        assert isinstance(transport, Transport), TypeError(f"{transport} type must{Transport}")
        self._transport = transport

    @property
    def is_close(self) -> bool:
        return self._connected_cnt <= 0

    async def create(
        self, ip: str, port: int, weight: int = 1, min_weight: Optional[int] = None, probability: int = 10
    ) -> None:
        if not self._transport:
            raise ConnectionError("conn manager need transport")

        if weight > 10:
            weight = 10
        if weight < 1:
            weight = 1
        if not min_weight:
            min_weight = 1
        else:
            if min_weight > weight:
                min_weight = weight
            if min_weight < 1:
                min_weight = 1

        key: str = f"{ip}:{port}"
        if key in self._conn_dict:
            raise ConnectionError(f"conn:{key} already create")

        if not (1 <= probability <= 10):
            raise ValueError(f"probability value must between 0 and 10")

        conn: Connection = Connection(
            ip, port, self._timeout, weight, min_weight, ssl_crt_path=self._ssl_crt_path, probability=probability
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
        conn.conn_id = await self._transport.declare(self.server_name, conn)
        self._connected_cnt += 1
        conn.listen_future = asyncio.ensure_future(self._transport.listen(conn))
        conn.listen_future.add_done_callback(lambda f: _conn_done(f))
        logging.debug(f"Connection to %s...", conn.connection_info)

        self._host_weight_list.extend([key for _ in range(weight)])
        random.shuffle(self._host_weight_list)

        self._conn_dict[key] = conn
        self._auto_select_conn_method()

    async def set_probability(self, ip: str, port: int, probability: int) -> None:
        key: str = f"{ip}:{port}"
        if key not in self._conn_dict:
            raise KeyError(f"Not found {key}")
        if not (1 < probability < 10):
            raise ValueError(f"probability value must between 0 and 10")
        self._conn_dict[key].probability = probability
        self._auto_select_conn_method()

    def _auto_select_conn_method(self) -> None:
        enable_probability_filter: bool = True
        if len(self._conn_dict) > 1:
            for conn in self._conn_dict.values():
                if conn.probability < 10:
                    enable_probability_filter = True
                    break

        if self._select_conn_method == SelectConnEnum.random:
            if enable_probability_filter:
                target_func: Callable = self._get_random_conn_by_probability
            else:
                target_func = self._get_random_conn
        else:
            if enable_probability_filter:
                target_func = self._get_round_robin_conn_by_probability
            else:
                target_func = self._get_round_robin_conn
        logging.debug(f"use get conn method:{target_func}")
        setattr(self, self.get_conn.__name__, target_func)

    async def destroy(self, ip: str, port: int) -> None:
        key: str = f"{ip}:{port}"
        if key not in self._conn_dict:
            return

        if not self.is_close:
            await self._conn_dict[key].await_close()

        self._host_weight_list = [i for i in self._host_weight_list if i != key]
        del self._conn_dict[key]

    async def start(self) -> None:
        raise NotImplementedError

    async def stop(self) -> None:
        """close all conn and cancel future"""
        for key, conn in self._conn_dict.copy().items():
            if not conn.is_closed():
                await conn.await_close()
        self._host_weight_list = []
        self._conn_dict = {}

    def get_conn(self) -> Connection:
        raise NotImplementedError

    def get_conn_list(self, cnt: Optional[int] = None) -> List[Connection]:
        if not cnt:
            cnt = self._connected_cnt // 3
        if cnt <= 0:
            cnt = 1
        return [self.get_conn() for _ in range(cnt)]

    def _get_random_conn(self) -> Connection:
        key = random.choice(self._host_weight_list)
        return self._conn_dict[key]

    def _get_random_conn_by_probability(self) -> Connection:
        conn_set: Set[Connection] = set()
        while True:
            key: str = random.choice(self._host_weight_list)
            conn: Connection = self._conn_dict[key]
            if conn in conn_set:
                continue
            if len(conn_set) == len(self._conn_dict):
                break
            if conn.is_available:
                return conn
            conn_set.add(conn)
        raise ValueError("Get conn fail")

    def _get_round_robin_conn(self) -> Connection:
        self._round_robin_index += 1
        index = self._round_robin_index % (len(self._host_weight_list))
        key = self._host_weight_list[index]
        return self._conn_dict[key]

    def _get_round_robin_conn_by_probability(self) -> Connection:
        conn_set: Set[Connection] = set()
        while True:
            self._round_robin_index += 1
            index = self._round_robin_index % (len(self._host_weight_list))
            key: str = self._host_weight_list[index]
            conn: Connection = self._conn_dict[key]
            if conn in conn_set:
                continue
            if len(conn_set) == len(self._conn_dict):
                break
            if conn.is_available:
                return conn
            conn_set.add(conn)
        raise ValueError("Get conn fail")

    def __len__(self) -> int:
        return self._connected_cnt
