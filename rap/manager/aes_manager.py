from typing import Dict, Optional, Union

from rap.common.aes import Crypto
from rap.common.utlis import MISS_OBJECT


class AesManager(object):
    # FIXME split key and secret
    def __init__(self):
        self._aes_dict: Dict[str, 'Crypto'] = {}

    def add_aes(self, key: str, crypto: Optional[Crypto] = None) -> 'Crypto':
        if crypto is None:
            crypto = Crypto(key)
        self._aes_dict[key] = crypto
        return crypto

    def get_aed(self, key: str) -> 'Union[Crypto, MISS_OBJECT]':
        return self._aes_dict.get(key, MISS_OBJECT)

    def remove_aes(self, key: str):
        if key in self._aes_dict:
            del self._aes_dict[key]


aes_manager: 'AesManager' = AesManager()
