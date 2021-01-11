import time

from rap.client.model import Request, Response
from rap.common.crypto import Crypto
from rap.common.exceptions import CryptoError
from rap.common.utlis import gen_random_time_id

from .base import BaseProcessor


class CryptoProcessor(BaseProcessor):
    def __init__(self, crypto_key_id: str, crypto_key: str):
        self._crypto_id: str = crypto_key_id
        self._crypto_key: str = crypto_key

        self._nonce_set: set = set()
        self._crypto: "Crypto" = Crypto(self._crypto_key)

    def _body_handle(self, body: dict):
        timestamp: int = body.get("timestamp", 0)
        if (int(time.time()) - timestamp) > 60:
            raise CryptoError("timeout error")
        nonce: str = body.get("nonce", "")
        if nonce in self._nonce_set:
            raise CryptoError("nonce error")
        else:
            self._nonce_set.add(nonce)

    async def process_request(self, request: Request):
        request.body = {"body": request.body, "timestamp": int(time.time()), "nonce": gen_random_time_id()}
        request.header["crypto_id"] = self._crypto_id
        request.body = self._crypto.encrypt_object(request.body)

    async def process_response(self, response: Response):
        try:
            if type(response.body) is bytes:
                response.body = self._crypto.decrypt_object(response.body)
                self._body_handle(response.body)
                response.body = response.body["body"]
        except Exception as e:
            raise CryptoError(f"Can't decrypt body.") from e
