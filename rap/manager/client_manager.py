import asyncio
import time
from dataclasses import dataclass, field
from typing import Dict, Generator, Optional

from rap.common.utlis import gen_random_time_id, MISS_OBJECT


@dataclass()
class ClientModel(object):
    client_id: Optional[str] = None
    generator_dict: Dict[int, Generator] = field(default_factory=dict)
    keep_alive_timestamp: int = int(time.time())
    ping_event_future: Optional[asyncio.Future] = None


class ClientManager(object):
    def __init__(self):
        self._client_dict: Dict[str, "ClientModel"] = {}

    def exist(self, client_id) -> bool:
        return client_id in self._client_dict

    def save_client_model(self, client_model: "ClientModel"):
        while True:
            client_id: str = gen_random_time_id(length=6)
            if client_id not in self._client_dict:
                self._client_dict[client_id] = client_model
            break
        client_model.client_id = client_id

    def get_client_model(self, client_id: str) -> "ClientModel":
        return self._client_dict.get(client_id, MISS_OBJECT)

    def destroy_client_model(self, client_id: str):
        if client_id in self._client_dict:
            del self._client_dict[client_id]

    async def async_destroy_client_model(self, client_id: str, second: int = 10):
        await asyncio.sleep(second)
        self.destroy_client_model(client_id)


client_manager: "ClientManager" = ClientManager()
