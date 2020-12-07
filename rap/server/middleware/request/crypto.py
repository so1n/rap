import time
from typing import Dict

from rap.common.utlis import Constant, MISS_OBJECT, gen_random_time_id
from rap.common.crypto import Crypto
from rap.common.exceptions import AuthError, ServerError
from rap.manager.crypto_manager import crypto_manager
from rap.manager.redis_manager import redis_manager
from rap.server.middleware.base import BaseRequestMiddleware
from rap.server.requests import RequestModel
from rap.server.response import ResponseModel


class CryptoMiddleware(BaseRequestMiddleware):
    def __init__(self, secret_dict: Dict[str, str]):
        crypto_manager.load_aes_key_dict(secret_dict)
        self._nonce_key: str = redis_manager.namespace + 'nonce'

    async def dispatch(self, request: RequestModel, response: ResponseModel) -> ResponseModel:
        if type(request.body) is bytes:
            if response.response_num == Constant.DECLARE_RESPONSE:
                client_id: str = request.header['client_id']
                crypto: Crypto = crypto_manager.get_crypto_by_key_id(client_id)
            else:
                client_id: str = request.client_model.client_id
                crypto: Crypto = crypto_manager.get_crypto_by_key(client_id)
            # check crypto
            if crypto == MISS_OBJECT:
                response.exception = AuthError("crypto key error")
                return response
            try:
                request.body = crypto.decrypt_object(request.body)
            except Exception:
                response.exception = AuthError("decrypt body error")
                return response

            try:
                timestamp: int = (request.body.get("timestamp", 0))
                if (int(time.time()) - timestamp) > 60:
                    response.exception = ServerError("timeout error")
                    return response
                nonce: str = request.body.get("nonce", "")
                if await redis_manager.redis_pool.sismember(self._nonce_key, nonce):
                    response.exception = ServerError("nonce error")
                    return response
                else:
                    await redis_manager.redis_pool.sadd(self._nonce_key, nonce)
                request.body = request.body['body']
            except Exception as e:
                response.exception = e
                return response

            response: ResponseModel = await self.call_next(request, response)
            if response.body:
                response.body.update(dict(timestamp=int(time.time()), nonce=gen_random_time_id()))
                response.body = crypto.encrypt_object(response.body)
            if response.response_num == Constant.DECLARE_RESPONSE:
                crypto_manager.add_crypto(request.client_model.client_id)
            return response
        else:
            return await self.call_next(request, response)
