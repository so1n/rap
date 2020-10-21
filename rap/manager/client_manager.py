from dataclasses import (
    dataclass,
    field
)
from enum import Enum, auto
from typing import Dict, Generator, Optional, Set

from rap.aes import Crypto
from rap.utlis import gen_id, MISS_OBJECT


class LifeCycleEnum(Enum):
    declare = auto()
    msg = auto()
    drop = auto()


@dataclass()
class ClientModel(object):
    client_id: Optional[str] = None
    crypto: Optional[Crypto] = None
    generator_dict: Dict[int, Generator] = field(default_factory=dict)
    life_cycle: 'LifeCycleEnum' = LifeCycleEnum.declare
    nonce_set: Set[str] = field(default_factory=set)


class ClientManager(object):
    def __init__(self):
        self._client_dict = {}
        self._expire_time: int = 1800

    @staticmethod
    def exist(self, client_id) -> bool:
        return client_id in self._client_dict

    def create_client_model(self, client_model: 'ClientModel'):
        while True:
            client_id: str = gen_id(6)
            if client_id not in self._client_dict:
                self._client_dict[client_id] = client_model
            break
        client_model.client_id = client_id

    def get_client_model(self, client_id: str) -> 'ClientModel':
        return self._client_dict.get(client_id, MISS_OBJECT)

    def destroy_client_model(self, client_id: str):
        if client_id in self._client_dict:
            del self._client_dict[client_id]


client_manager: 'ClientManager' = ClientManager()
