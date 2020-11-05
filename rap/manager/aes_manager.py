from typing import Dict, Optional, Union

from rap.common.aes import Crypto
from rap.common.utlis import MISS_OBJECT


class AesManager(object):
    def __init__(self):
        self._aes_key_dict: Dict[str, str] = {}
        self._aes_crypto_dict: Dict[str, "Crypto"] = {}

    def load_aes_key_dict(self, aes_key_dict: Dict[str, str]):
        self._aes_key_dict = aes_key_dict
        for key, value in aes_key_dict.items():
            self.add_crypto(value)

    def add_crypto(self, key: str, crypto: Optional[Crypto] = None) -> "Crypto":
        if crypto is None:
            crypto = Crypto(key)
        self._aes_crypto_dict[key] = crypto
        return crypto

    def get_crypto(self, key: str) -> "Union[Crypto, MISS_OBJECT]":
        aes_key: str = self._aes_key_dict.get(key, "")
        return self._aes_crypto_dict.get(aes_key, MISS_OBJECT)

    def remove_aes(self, key: str):
        if key in self._aes_crypto_dict:
            del self._aes_crypto_dict[key]


aes_manager: "AesManager" = AesManager()
