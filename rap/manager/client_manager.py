from dataclasses import (
    dataclass,
    field
)
from typing import Dict, Generator, Optional

from rap.aes import Crypto
from rap.utlis import MISS_OBJECT


@dataclass()
class ClientModel(object):
    client_id: Optional[str] = None
    crypto: Optional[Crypto] = None
    generator_dict: Dict[int, Generator] = field(default_factory=dict)


class ClientManager(object):
    def __init__(self):
        self._client_dict = {}
    
    def exist(self, client_id) -> bool:
        return client_id in self._client_dict

    def create_client_info(self, client_id: str, client_model: 'ClientModel') -> bool:
        if self.exist(client_id):
            return False
        self._client_dict[client_id] = client_model
        return True

    def get_client_info(self, client_id: str) -> 'ClientModel':
        return self._client_dict.get(client_id, MISS_OBJECT)

    def destroy_client_info(self, client_id: str):
        if client_id in self._client_dict:
            del self._client_dict[client_id]


client_manager: 'ClientManager' = ClientManager()
