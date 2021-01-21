from typing import Dict, Optional, Union, TYPE_CHECKING

from rap.common.crypto import Crypto
from rap.common.utlis import MISS_OBJECT

if TYPE_CHECKING:
    from rap.server.core import Server


class CryptoManager(object):
    def __init__(self, app: "Server"):
        self.app: "Server" = app
        self._key_dict: Dict[str, str] = {}
        self._crypto_dict: Dict[str, "Crypto"] = {}

        self.app.register(self.load_aes_key_dict, group="root")
        self.app.register(self.remove_aes, group="root")

    def load_aes_key_dict(self, aes_key_dict: Dict[str, str]) -> None:
        self._key_dict = aes_key_dict
        for key, value in aes_key_dict.items():
            self._key_dict[key] = value
            self._crypto_dict[value] = Crypto(value)

    def get_crypto_by_key_id(self, key_id: str) -> "Union[Crypto, MISS_OBJECT]":
        key: str = self._key_dict.get(key_id, "")
        return self._crypto_dict.get(key, MISS_OBJECT)

    def get_crypto_by_key(self, key: str) -> "Union[Crypto, MISS_OBJECT]":
        return self._crypto_dict.get(key, MISS_OBJECT)

    def remove_aes(self, key: str) -> None:
        if key in self._crypto_dict:
            del self._crypto_dict[key]

    def __bool__(self) -> bool:
        return bool(self._crypto_dict)