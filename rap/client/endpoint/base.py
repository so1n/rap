import asyncio
import logging
import random
from enum import Enum, auto
from typing import Dict, List, Optional, Set

from rap.client.transport.transport import Transport
from rap.common.conn import Connection


class SelectConnEnum(Enum):
    random = auto()
    round_robin = auto()


class BaseEndpoint(object):
    def __init__(
        self,
        server_name: str,
        timeout: Optional[int] = None,
        ssl_crt_path: Optional[str] = None,
        select_conn_method: Optional[SelectConnEnum] = None,
        pack_param: Optional[dict] = None,
        unpack_param: Optional[dict] = None,
    ) -> None:
        """
        server_name: server name
        conn_list: client conn info
          include ip, port, weight
          ip: server ip
          port: server port
          weight: select this conn weight
          e.g.  [{"ip": "localhost", "port": "9000", weight: 10}]
        timeout: read response from consumer timeout
        """
        self.server_name: str = server_name
        self._transport: Optional[Transport] = None
        self._host_weight_list: List[str] = []
        self._timeout: int = timeout or 1200
        self._ssl_crt_path: Optional[str] = ssl_crt_path
        self._pack_param: Optional[dict] = pack_param
        self._unpack_param: Optional[dict] = unpack_param

        self._connected_cnt: int = 0
        self._conn_dict: Dict[str, Connection] = {}
        self._round_robin_index: int = 0
        self._is_close: bool = True

        if not select_conn_method:
            setattr(self, self.get_conn.__name__, self._get_random_conn)
        elif select_conn_method == SelectConnEnum.random:
            setattr(self, self.get_conn.__name__, self._get_random_conn)
        else:
            setattr(self, self.get_conn.__name__, self._get_round_robin_conn)

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
        await self._transport.declare(self.server_name, conn)
        self._connected_cnt += 1
        conn.listen_future = asyncio.ensure_future(self._transport.listen(conn))
        conn.listen_future.add_done_callback(lambda f: _conn_done(f))
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

    def get_conn(self) -> Connection:
        """get conn by endpoint"""

    def get_conn_list(self, cnt: Optional[int] = None) -> List[Connection]:
        """get conn list by endpoint
        cnt: How many conn to get.
          if the value is empty, it will automatically get 1/3 of the conn from the endpoint,
          which should not be less than or equal to 0
        """
        if not cnt:
            cnt = self._connected_cnt // 3
        if cnt <= 0:
            cnt = 1
        conn_set: Set[Connection] = set()
        while len(conn_set) < cnt:
            conn_set.add(self.get_conn())
        return list(conn_set)

    def _get_random_conn(self) -> Connection:
        """random get conn"""
        if not self._host_weight_list:
            raise ConnectionError("Endpoint Can not found conn")
        key = random.choice(self._host_weight_list)
        return self._conn_dict[key]

    def _get_round_robin_conn(self) -> Connection:
        """get conn by round robin"""
        if not self._host_weight_list:
            raise ConnectionError("Endpoint Can not found conn")
        self._round_robin_index += 1
        index = self._round_robin_index % (len(self._host_weight_list))
        key = self._host_weight_list[index]
        return self._conn_dict[key]

    def __len__(self) -> int:
        return self._connected_cnt
