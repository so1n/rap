try:
    import ujson as json
except ModuleNotFoundError:
    import json

from binascii import a2b_hex, b2a_hex
from typing import Any

from Crypto.Cipher import AES


class Crypto(object):
    def __init__(self, key: str):
        """crypto key, len must 16"""
        self.key: str = key
        self._length = 16
        self._mode: "AES.MODE_CBC" = AES.MODE_CBC

    def encrypt(self, raw_data: str) -> bytes:
        """encrypt str to bytes"""
        new_crypto: "AES.new" = AES.new(self.key, self._mode, self.key)
        count: int = len(raw_data)
        salt: int = 0
        if count % self._length != 0:
            salt = self._length - (count % self._length)
        raw_data: str = raw_data + ("\0" * salt)
        encrypt_str: str = new_crypto.encrypt(raw_data)
        return b2a_hex(encrypt_str)

    def decrypt(self, raw_byte: bytes) -> str:
        """decrypt bytes to str"""
        new_crypto: "AES.new" = AES.new(self.key, self._mode, self.key)
        decrypt_pt: str = new_crypto.decrypt(a2b_hex(raw_byte)).decode()
        return decrypt_pt.rstrip("\0")

    def encrypt_object(self, _object: Any) -> bytes:
        return self.encrypt(json.dumps(_object))

    def decrypt_object(self, raw_byte: bytes) -> Any:
        return json.loads(self.decrypt(raw_byte))


if __name__ == "__main__":
    crypto: "Crypto" = Crypto("keyskeyskeyskeys")
    raw_str: str = "test_rap_crypto_text"

    encrypt_byte: bytes = crypto.encrypt(raw_str)
    decrypt_str: str = crypto.decrypt(encrypt_byte)
    print(raw_str)
    print(encrypt_byte)
    print(decrypt_str)
    print(raw_str == decrypt_str)
