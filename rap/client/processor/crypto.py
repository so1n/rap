import time
from typing import Dict

from rap.client.model import Request, Response
from rap.common.crypto import Crypto
from rap.common.exceptions import CryptoError
from rap.common.utils import gen_random_time_id, get_event_loop

from .base import BaseProcessor


class AutoExpireSet(object):
    def __init__(self, interval: float = 10.0):
        self._dict: Dict[str, float] = {}
        self._interval: float = interval

    def _add(self, key: str, expire: float) -> None:
        self._dict[key] = time.time() + expire

    def add(self, key: str, expire: float) -> None:
        self._add(key, expire)
        if get_event_loop().is_running():
            self._auto_remove()
            setattr(self, self.add.__name__, self._add)

    def __contains__(self, key: str) -> bool:
        if key not in self._dict:
            return False
        elif self._dict[key] < time.time():
            del self._dict[key]
            return False
        else:
            return True

    def _auto_remove(self) -> None:
        now_timestamp: float = time.time()
        for key in list(self._dict.keys()):
            if key not in self._dict:
                continue
            elif self._dict[key] < now_timestamp:
                del self._dict[key]

        get_event_loop().call_later(self._interval, self._auto_remove)


class CryptoProcessor(BaseProcessor):
    def __init__(self, crypto_key_id: str, crypto_key: str, timeout: int = 60, interval: int = 120):
        self._crypto_id: str = crypto_key_id
        self._crypto_key: str = crypto_key
        self._timeout: int = timeout

        self._nonce_set: AutoExpireSet = AutoExpireSet(interval=interval)
        self._crypto: "Crypto" = Crypto(self._crypto_key)

    def _body_handle(self, body: dict) -> None:
        timestamp: int = body.get("timestamp", 0)
        if (int(time.time()) - timestamp) > 60:
            raise CryptoError("timeout error")
        nonce: str = body.get("nonce", "")
        if nonce in self._nonce_set:
            raise CryptoError("nonce error")
        else:
            self._nonce_set.add(nonce, self._timeout)

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
