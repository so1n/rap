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
    def __init__(self, secret_dict: Dict[str, str]):
        crypto_manager.load_aes_key_dict(secret_dict)
        self._nonce_key: str = redis_manager.namespace + "nonce"

    async def process_request(self, request: RequestModel):
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
            if await redis_manager.redis_pool.sismember(self._nonce_key, nonce):
                raise ServerError("nonce error")
            else:
                await redis_manager.redis_pool.sadd(self._nonce_key, nonce)
            request.body = request.body["body"]
            request.stats.crypto = crypto
        except Exception as e:
            raise CryptoError(str(e)) from e

    async def process_response(self, response: ResponseModel):
        if response.body and response.num != Constant.SERVER_ERROR_RESPONSE:
            try:
                crypto: Crypto = response.stats.crypto
            except AttributeError:
                return
            if type(response.body) is not dict:
                response.body = {"body": response.body}
            response.body.update(dict(timestamp=int(time.time()), nonce=gen_random_time_id()))
            response.body = crypto.encrypt_object(response.body)
