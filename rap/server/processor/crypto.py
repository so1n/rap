import time
from typing import Callable, Dict, List, Optional, Union

from aredis import StrictRedis, StrictRedisCluster  # type: ignore

from rap.common.crypto import Crypto
from rap.common.exceptions import CryptoError, ParseError
from rap.common.utils import Constant, gen_random_time_id
from rap.server.model import Request, Response
from rap.server.processor.base import BaseProcessor


class CryptoProcessor(BaseProcessor):
    def __init__(
        self,
        secret_dict: Dict[str, str],
        redis: Union[StrictRedis, StrictRedisCluster],
        timeout: int = 60,
        nonce_timeout: int = 120,
    ):
        self._timeout: int = timeout
        self._nonce_timeout: int = nonce_timeout
        self._nonce_key = f"{self.__class__.__name__}:nonce_key"

        self._redis: Union[StrictRedis, StrictRedisCluster] = redis
        self._key_dict: Dict[str, str] = {}
        self._crypto_dict: Dict[str, "Crypto"] = {}

        self.load_aes_key_dict(secret_dict)

    def start_event_handle(self) -> None:
        self.register(self.modify_crypto_timeout)
        self.register(self.modify_crypto_nonce_timeout)

        self.register(self.get_crypto_key_id_list)
        self.register(self.load_aes_key_dict)
        self.register(self.remove_aes)

    def load_aes_key_dict(self, aes_key_dict: Dict[str, str]) -> None:
        """load aes key dict. eg{'key_id': 'xxxxxxxxxxxxxxxx'}"""
        for key, value in aes_key_dict.items():
            self._key_dict[key] = value
            self._crypto_dict[value] = Crypto(value)

    def get_crypto_key_id_list(self) -> List[str]:
        """get crypto key in list"""
        return list(self._key_dict.keys())

    def get_crypto_by_key_id(self, key_id: str) -> "Optional[Crypto]":
        key: str = self._key_dict.get(key_id, "")
        return self._crypto_dict.get(key, None)

    def get_crypto_by_key(self, key: str) -> "Optional[Crypto]":
        return self._crypto_dict.get(key, None)

    def remove_aes(self, key: str) -> None:
        """delete aes value by key"""
        if key in self._key_dict:
            value: str = self._key_dict[key]
            del self._crypto_dict[value]
            del self._key_dict[key]

    def modify_crypto_timeout(self, timeout: int) -> None:
        """modify crypto timeout param"""
        self._timeout = timeout

    def modify_crypto_nonce_timeout(self, timeout: int) -> None:
        """modify crypto nonce timeout param"""
        self._nonce_timeout = timeout

    async def process_request(self, request: Request) -> Request:
        """decrypt request body"""
        if type(request.body) is not bytes:
            return request
        crypto_id: str = request.header.get("crypto_id", None)
        crypto: Optional[Crypto] = self.get_crypto_by_key_id(crypto_id)
        # check crypto
        if crypto is None:
            raise CryptoError("crypto id error")
        try:
            request.body = crypto.decrypt_object(request.body)
        except Exception as e:
            raise CryptoError("decrypt body error") from e

        try:
            timestamp: int = request.body.get("timestamp", 0)
            if (int(time.time()) - timestamp) > self._timeout:
                raise ParseError(extra_msg="timeout param error")
            nonce: str = request.body.get("nonce", "")
            if not nonce:
                raise ParseError(extra_msg="nonce param error")
            nonce = f"{self._nonce_key}:{nonce}"
            if await self._redis.exists(nonce):
                raise ParseError(extra_msg="nonce param error")
            else:
                await self._redis.set(nonce, 1, ex=self._nonce_timeout)
            request.body = request.body["body"]

            # set share data
            request.stats.crypto = crypto

            return request
        except Exception as e:
            raise CryptoError(str(e)) from e

    async def process_response(self, response: Response) -> Response:
        """encrypt response body"""
        if (
            response.header.get("status_code") == 200
            and response.body
            and response.num != Constant.SERVER_ERROR_RESPONSE
        ):
            try:
                crypto: Crypto = response.stats.crypto
            except AttributeError:
                return response
            response.body = {"body": response.body, "timestamp": int(time.time()), "nonce": gen_random_time_id()}
            response.body = crypto.encrypt_object(response.body)
        return response
