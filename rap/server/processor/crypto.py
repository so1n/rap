import time
from typing import Dict

from rap.common.crypto import Crypto
from rap.common.exceptions import CryptoError, ServerError
from rap.common.utlis import MISS_OBJECT, Constant, gen_random_time_id
from rap.manager.crypto_manager import crypto_manager
from rap.manager.redis_manager import redis_manager
from rap.server.model import RequestModel, ResponseModel
from rap.server.processor.base import BaseProcessor


class CryptoProcessor(BaseProcessor):
    def __init__(self, secret_dict: Dict[str, str] = None, timeout: int = 60, nonce_timeout: int = 60):
        if not secret_dict and not crypto_manager:
            raise ValueError("secret_dict must not None")
        crypto_manager.load_aes_key_dict(secret_dict)
        self._nonce_key: str = redis_manager.namespace + "nonce"
        self._timeout: int = timeout
        self._nonce_timeout: int = nonce_timeout

        self.register(self.modify_timeout)
        self.register(self.modify_nonce_timeout)

    @staticmethod
    def add_secret_dict(secret_dict: Dict[str, str]):
        crypto_manager.load_aes_key_dict(secret_dict)

    def modify_timeout(self, timeout: int) -> None:
        self._timeout = timeout

    def modify_nonce_timeout(self, timeout: int) -> None:
        self._nonce_timeout = timeout

    async def process_request(self, request: RequestModel):
        """decrypt request body"""
        if type(request.body) is not bytes:
            return
        crypto_id: str = request.header.get("crypto_id", None)
        crypto: Crypto = crypto_manager.get_crypto_by_key_id(crypto_id)
        # check crypto
        if crypto == MISS_OBJECT:
            raise CryptoError("crypto id error")
        try:
            request.body = crypto.decrypt_object(request.body)
        except Exception as e:
            raise CryptoError("decrypt body error") from e

        try:
            timestamp: int = request.body.get("timestamp", 0)
            if (int(time.time()) - timestamp) > 60:
                raise ServerError("timeout error")
            nonce: str = request.body.get("nonce", "")
            if not nonce:
                raise ServerError("nonce error")
            nonce = f"{self._nonce_key}:{nonce}"
            if await redis_manager.exists(nonce):
                raise ServerError("nonce error")
            else:
                await redis_manager.redis_pool.set(nonce, 1, expire=self._nonce_timeout)
            request.body = request.body["body"]

            # set share data
            request.stats.crypto = crypto
        except Exception as e:
            raise CryptoError(str(e)) from e

    async def process_response(self, response: ResponseModel):
        """encrypt response body"""
        if response.body and response.num != Constant.SERVER_ERROR_RESPONSE:
            try:
                crypto: Crypto = response.stats.crypto
            except AttributeError:
                return
            response.body = {"body": response.body, "timestamp": int(time.time()), "nonce": gen_random_time_id()}
            response.body = crypto.encrypt_object(response.body)
