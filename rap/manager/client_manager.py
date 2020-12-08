import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Dict, Generator, Optional

from rap.common.utlis import gen_random_time_id, MISS_OBJECT


class LifeCycleEnum(Enum):
    declare = auto()
    msg = auto()
    drop = auto()


@dataclass()
class ClientModel(object):
    client_id: Optional[str] = None
    generator_dict: Dict[int, Generator] = field(default_factory=dict)
    _life_cycle: "LifeCycleEnum" = LifeCycleEnum.declare
    keep_alive_timestamp: int = int(time.time())
    ping_event_future: Optional[asyncio.Future] = None

    def modify_life_cycle(self, life_cycle: "LifeCycleEnum") -> bool:
        now_life_cycle: "LifeCycleEnum" = self._life_cycle
        result: bool = False
        if now_life_cycle is LifeCycleEnum.msg and life_cycle is LifeCycleEnum.msg:
            result = True
        elif now_life_cycle is LifeCycleEnum.declare and life_cycle is LifeCycleEnum.msg:
            result = True
        elif now_life_cycle is not LifeCycleEnum.drop and life_cycle is LifeCycleEnum.drop:
            result = True
        return result


class ClientManager(object):
    def __init__(self):
        self._client_dict: Dict[str, "ClientModel"] = {}
        self._expire_time: int = 1800

    def exist(self, client_id) -> bool:
        return client_id in self._client_dict

    def create_client_model(self, client_model: "ClientModel"):
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
