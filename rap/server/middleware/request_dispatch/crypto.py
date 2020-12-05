import time
from typing import Dict

from rap.common.utlis import Constant, MISS_OBJECT, gen_random_time_id
from rap.common.crypto import Crypto
from rap.common.exceptions import AuthError, ServerError
from rap.manager.crypto_manager import crypto_manager
from rap.server.middleware.base import BaseRequestDispatchMiddleware
from rap.server.requests import RequestModel
from rap.server.response import ResponseModel


class CryptoMiddleware(BaseRequestDispatchMiddleware):
    def __init__(self, secret_dict: Dict[str, str]):
        crypto_manager.load_aes_key_dict(secret_dict)
        # TODO redis
        self._nonce_set: set = set()

    def _body_handle(self, body: dict):
        timestamp: int = (body.get("timestamp", 0))
        if (int(time.time()) - timestamp) > 60:
            raise ServerError("timeout error")
        nonce: str = body.get("nonce", "")
        if nonce in self._nonce_set:
            raise ServerError("nonce error")
        else:
            self._nonce_set.add(nonce)

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
                self._body_handle(request.body)
                request.body = request.body['body']
            except Exception as e:
                response.exception = e
                return response

            response: ResponseModel = await self.call_next(request, response)
            if response.result:
                response.result.update(dict(timestamp=int(time.time()), nonce=gen_random_time_id()))
                response.result = crypto.encrypt_object(response.result)
            if response.response_num == Constant.DECLARE_RESPONSE:
                crypto_manager.add_crypto(request.client_model.client_id)
            return response
        else:
            return await self.call_next(request, response)
