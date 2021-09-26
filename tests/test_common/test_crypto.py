import pytest

from rap.common.crypto import Crypto

pytestmark = pytest.mark.asyncio


class TestCrypto:
    def test_init_error(self) -> None:
        with pytest.raises(ValueError):
            Crypto("keyskeyskeyskey")

    def test_crypto_str(self) -> None:
        crypto: "Crypto" = Crypto("keyskeyskeyskeys")
        raw_str: str = "test_rap_crypto_text"

        encrypt_byte: bytes = crypto.encrypt(raw_str)
        decrypt_str: str = crypto.decrypt(encrypt_byte)
        assert raw_str == decrypt_str

    def test_crypto_obj(self) -> None:
        crypto: "Crypto" = Crypto("keyskeyskeyskeys")
        raw_obj: dict = {"key": "test"}

        encrypt_byte: bytes = crypto.encrypt_object(raw_obj)
        decrypt_obj: dict = crypto.decrypt_object(encrypt_byte)
        assert raw_obj == decrypt_obj
