import time
import uuid
from typing import Optional, Tuple

from rap.client.model import Request, Response
from rap.client.processor.base import BaseProcessor
from rap.common.crypto import Crypto
from rap.common.exceptions import CryptoError, RPCError
from rap.common.utils import Constant, gen_random_time_id


class BaseCryptoProcessor(BaseProcessor):
    _nonce_timeout: int = 60

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


class AutoCryptoProcessor(BaseCryptoProcessor):
    def __init__(self, nonce_timeout: Optional[int] = None):
        """
        nonce_time: Cache nonce time, each message has a nonce field, and the value of each message is different,
            which is used to prevent message re-attack.
        """
        self._nonce_timeout: int = nonce_timeout or 60

    async def process_request(self, request: Request) -> Request:
        key: str = f"{self.__class__.__name__}:{request.header['host']}"
        if request.msg_type == Constant.CLIENT_EVENT and request.target.endswith(Constant.DECLARE):
            check_id: str = gen_random_time_id(length=6, time_length=10)
            crypto_id: str = str(uuid.uuid4())
            request.body["crypto_id"] = crypto_id
            request.body["crypto_key"] = gen_random_time_id(length=6, time_length=10)
            request.body["check_id"] = check_id
            request.app.cache.add(key, 60, (crypto_id, check_id))
        elif request.msg_type in (Constant.MSG_REQUEST, Constant.CHANNEL_REQUEST):
            try:
                if not request.conn:
                    raise RPCError("Not found conn from request")
                crypto_id = getattr(request.conn, "_crypto_id", "")
                crypto: Optional[Crypto] = getattr(request.conn, "_crypto", None)
                if not crypto_id or not crypto:
                    raise CryptoError(f"conn:{request.conn} not support crypto")
                request.body = {"body": request.body, "timestamp": int(time.time()), "nonce": gen_random_time_id()}
                request.header["crypto_id"] = crypto_id
                request.body = crypto.encrypt_object(request.body)
            except Exception as e:
                raise CryptoError(f"Can't decrypt body.") from e
        return request

    async def process_response(self, response: Response) -> Response:
        key: str = f"{self.__class__.__name__}:{response.conn.sock_tuple[0]}"
        try:
            if response.msg_type == Constant.SERVER_EVENT and response.target.endswith(Constant.DECLARE):
                result: Optional[Tuple[str, str]] = response.app.cache.get_and_update_expire(key, 60 * 5)
                if not result:
                    raise CryptoError(f"{response.header['host']} not create crypto")
                _crypto_id, _check_id = result
                crypto_id: str = response.body.get("crypto_id", "")
                crypto_key: str = response.body.get("crypto_key", "")
                check_body: bytes = response.body.get("check_body", b"")
                if not crypto_id or not crypto_key or not check_body:
                    raise CryptoError("crypto param error")

                crypto: Crypto = Crypto(crypto_key)
                if crypto_id != crypto.decrypt_object(check_body) or crypto_id != _crypto_id:
                    raise CryptoError("crypto check error")
                setattr(response.conn, "_crypto_id", crypto_id)
                setattr(response.conn, "_crypto", crypto)
                return response
            if type(response.body) is bytes:
                crypto_id = getattr(response.conn, "_crypto_id", "")
                crypto = getattr(response.conn, "_crypto", None)
                if not crypto_id or not crypto:
                    raise CryptoError(f"conn:{response.conn} not support crypto")
                response.body = crypto.decrypt_object(response.body)
                self._body_handle(response.body)
                response.body = response.body["body"]
            return response
        except Exception as e:
            raise CryptoError(f"Can't decrypt body.") from e


class CryptoProcessor(BaseCryptoProcessor):
    """Provide symmetric encryption and prevent message replay attacks"""

    def __init__(self, crypto_key_id: str, crypto_key: str, nonce_timeout: Optional[int] = None):
        """
        crypto_key_id: crypto_key id, Client and server identify crypto_key by id
        crypto_key: crypto key, Encrypt and decrypt messages
        nonce_time: Cache nonce time, each message has a nonce field, and the value of each message is different,
            which is used to prevent message re-attack.
        """
        self._crypto_id: str = crypto_key_id
        self._crypto_key: str = crypto_key
        self._nonce_timeout: int = nonce_timeout or 60

        self._crypto: "Crypto" = Crypto(self._crypto_key)

    async def process_request(self, request: Request) -> Request:
        if request.msg_type in (Constant.MSG_REQUEST, Constant.CHANNEL_REQUEST):
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
