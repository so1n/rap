import asyncio
import logging
import random
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Dict, List, Optional

from rap.common.conn import Connection
from rap.client.transport.transport import Transport


class SelectConnEnum(Enum):
    random = auto()
    round_robin = auto()


@dataclass()
class ConnModel(object):
    conn: Connection
    future: asyncio.Future = field(default_factory=asyncio.Future)
    weight: int = 0
    min_weight: int = 0
    probability: float = 1

    def __post_init__(self) -> None:
        self.future.set_result(True)

    def is_closed(self) -> bool:
        return self.conn.is_closed() or self.future.done()

    async def await_close(self) -> None:
        if not self.future.cancelled():
            self.future.cancel()
        if not self.conn.is_closed():
            await self.conn.await_close()


class BaseEnpoints(object):
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
        self._conn_dict: Dict[str, ConnModel] = {}
        self._round_robin_index: int = 0

        if select_conn_method == select_conn_method.random:
            setattr(self, self.get_conn.__name__, self.get_random_conn)
        else:
            setattr(self, self.get_conn.__name__, self.get_round_robin_conn)

    def set_transport(self, transport: Transport) -> None:
        assert isinstance(transport, Transport), TypeError(f"{transport} type must{Transport}")
        self._transport = transport

    @property
    def is_close(self) -> bool:
        return self._connected_cnt <= 0

    async def create(self, ip: str, port: int, weight: int = 1, min_weight: Optional[int] = None) -> None:
        if not self._transport:
            raise ConnectionError("conn manager need transport")

        if not min_weight or min_weight >= 1:
            min_weight = 1
        key: str = f"{ip}:{port}"
        if key in self._conn_dict:
            raise ConnectionError(f"conn:{key} already create")

        conn_model: ConnModel = ConnModel(
            Connection(ip, port, self._timeout, ssl_crt_path=self._ssl_crt_path),
            weight=weight,
            min_weight=min_weight,
        )

        def _conn_done(f: asyncio.Future) -> None:
            try:
                if key in self._conn_dict:
                    del self._conn_dict[key]
                    self._host_weight_list.remove(key)
                self._connected_cnt -= 1
            except Exception as e:
                logging.exception(f"close conn error: {e}")

        await conn_model.conn.connect()
        conn_model.conn.conn_id = await self._transport.declare(self.server_name, conn_model.conn)
        self._connected_cnt += 1
        conn_model.future = asyncio.ensure_future(self._transport.listen(conn_model.conn))
        conn_model.future.add_done_callback(lambda f: _conn_done(f))
        logging.debug(f"Connection to %s...", conn_model.conn.connection_info)

        self._host_weight_list.extend([key for _ in range(conn_model.weight)])
        random.shuffle(self._host_weight_list)

        self._conn_dict[key] = conn_model

    async def destroy(self, ip: str, port: int) -> None:
        key: str = f"{ip}:{port}"
        if key not in self._conn_dict:
            return

        if not self.is_close:
            conn_model: ConnModel = self._conn_dict[key]
            await conn_model.await_close()

        self._host_weight_list = [i for i in self._host_weight_list if i != key]
        del self._conn_dict[key]

    async def start(self) -> None:
        raise NotImplementedError

    async def stop(self) -> None:
        """close all conn and cancel future"""
        for key, conn_model in self._conn_dict.copy().items():
            if not conn_model.is_closed():
                await conn_model.await_close()
        self._host_weight_list = []
        self._conn_dict = {}

    def get_conn(self) -> Connection:
        raise NotImplementedError

    def get_random_conn(self) -> Connection:
        key: str = random.choice(self._host_weight_list)
        return self._conn_dict[key].conn

    def get_round_robin_conn(self) -> Connection:
        self._round_robin_index += 1
        index = self._round_robin_index % (len(self._host_weight_list))
        key: str = self._host_weight_list[index]
        return self._conn_dict[key].conn
