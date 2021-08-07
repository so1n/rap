import time
from typing import Dict

from rap.client.model import Request, Response
from rap.client.processor.base import BaseProcessor
from rap.common.crypto import Crypto
from rap.common.exceptions import CryptoError
from rap.common.utils import gen_random_time_id, get_event_loop


class CryptoProcessor(BaseProcessor):
    """Provide symmetric encryption and prevent message replay attacks"""

    def __init__(self, crypto_key_id: str, crypto_key: str, nonce_timeout: int = 60):
        """
        crypto_key_id: crypto_key id, Client and server identify crypto_key by id
        crypto_key: crypto key, Encrypt and decrypt messages
        nonce_time: Cache nonce time, each message has a nonce field, and the value of each message is different,
            which is used to prevent message re-attack.
        """
        self._crypto_id: str = crypto_key_id
        self._crypto_key: str = crypto_key
        self._nonce_timeout: int = nonce_timeout

        self._crypto: "Crypto" = Crypto(self._crypto_key)

    def _body_handle(self, body: dict) -> None:
        """Check if the message has timed out or has been received"""
        timestamp: int = body.get("timestamp", 0)
        if (int(time.time()) - timestamp) > 60:
            raise CryptoError("timeout param error")
        nonce: str = body.get("nonce", "")
        if not nonce:
            raise CryptoError("nonce param error")
        nonce = f"{self.__class__.__name__}:{nonce}"
        if nonce in self.app.cache:
            raise CryptoError("nonce param error")
        else:
            self.app.cache.add(nonce, self._nonce_timeout)

    async def process_request(self, request: Request) -> Request:
        request.body = {"body": request.body, "timestamp": int(time.time()), "nonce": gen_random_time_id()}
        request.header["crypto_id"] = self._crypto_id
        request.body = self._crypto.encrypt_object(request.body)
        return request

    async def process_response(self, response: Response) -> Response:
        try:
            if type(response.body) is bytes:
                response.body = self._crypto.decrypt_object(response.body)
                self._body_handle(response.body)
                response.body = response.body["body"]
            return response
        except Exception as e:
            raise CryptoError(f"Can't decrypt body.") from e
