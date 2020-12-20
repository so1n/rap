from typing import Dict, Optional, Union

from rap.common.crypto import Crypto
from rap.common.utlis import MISS_OBJECT


class CryptoManager(object):
    def __init__(self):
        self._key_dict: Dict[str, str] = {}
        self._crypto_dict: Dict[str, "Crypto"] = {}

    def load_aes_key_dict(self, aes_key_dict: Dict[str, str]):
        self._key_dict = aes_key_dict
        for key, value in aes_key_dict.items():
            self.add_crypto(value)

    def add_crypto(self, key: str, crypto: Optional[Crypto] = None) -> "Crypto":
        if crypto is None:
            crypto = Crypto(key)
        self._crypto_dict[key] = crypto
        return crypto

    def get_crypto_by_key_id(self, key_id: str) -> "Union[Crypto, MISS_OBJECT]":
        key: str = self._key_dict.get(key_id, "")
        return self._crypto_dict.get(key, MISS_OBJECT)

    def get_crypto_by_key(self, key: str) -> "Union[Crypto, MISS_OBJECT]":
        return self._crypto_dict.get(key, MISS_OBJECT)

    def remove_aes(self, key: str):
        if key in self._crypto_dict:
            del self._crypto_dict[key]


crypto_manager: "CryptoManager" = CryptoManager()
