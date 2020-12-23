import asyncio
import time
from dataclasses import dataclass, field
from typing import Dict, Generator, Optional

from rap.common.utlis import MISS_OBJECT


@dataclass()
class ConnDataModel(object):
    peer: str
    generator_dict: Dict[int, Generator] = field(default_factory=dict)
    keep_alive_timestamp: int = field(default_factory=lambda: int(time.time()))
    ping_event_future: Optional[asyncio.Future] = None

    def __del__(self):
        if self.ping_event_future and not self.ping_event_future.cancelled():
            self.ping_event_future.cancel()


class ConnDataManager(object):
    def __init__(self):
        self._conn_data_dict: Dict[str, "ConnDataModel"] = {}

    def exist(self, peer) -> bool:
        return peer in self._conn_data_dict

    def save_conn_data(self, conn_data_model: "ConnDataModel"):
        self._conn_data_dict[conn_data_model.peer] = conn_data_model

    def get_conn_data(self, peer: str) -> "ConnDataModel":
        return self._conn_data_dict.get(peer, MISS_OBJECT)

    def destroy_conn_data(self, peer: str):
        if peer in self._conn_data_dict:
            del self._conn_data_dict[peer]

    async def async_destroy_conn_data(self, peer: str, second: int = 10):
        if peer in self._conn_data_dict:
            await asyncio.sleep(second)
            self.destroy_conn_data(peer)


conn_data_manager: "ConnDataManager" = ConnDataManager()
