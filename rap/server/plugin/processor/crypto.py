import time
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

from rap.common.crypto import Crypto
from rap.common.exceptions import CryptoError, ParseError, RPCError
from rap.common.utils import Constant, EventEnum, gen_random_time_id
from rap.server.model import Request, Response
from rap.server.plugin.processor.base import BaseProcessor

if TYPE_CHECKING:
    from rap.server.core import Server
    from rap.server.types import SERVER_EVENT_FN


class BaseCryptoProcessor(BaseProcessor):
    """Provide symmetric encryption and prevent message replay attacks"""

    def __init__(
        self,
        secret_dict: Dict[str, str],
        timeout: int = 60,
        nonce_timeout: int = 120,
    ):
        self._timeout: int = timeout
        self._nonce_timeout: int = nonce_timeout
        self._nonce_key = f"{self.__class__.__name__}:nonce_key"

        self._key_dict: Dict[str, str] = {}
        self._crypto_dict: Dict[str, "Crypto"] = {}

        self.server_event_dict: Dict[EventEnum, List["SERVER_EVENT_FN"]] = {
            EventEnum.before_start: [self.start_event_handle]
        }
        self.load_aes_key_dict(secret_dict)

    def start_event_handle(self, app: "Server") -> None:
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

    def del_crypto_by_key_id(self, key_id: str) -> None:
        key: str = self._key_dict.get(key_id, "")
        if key:
            self._key_dict.pop(key, None)
            self._crypto_dict.pop(key, None)
        return

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

    async def decrypt_request(self, request: Request) -> Request:
        """decrypt request body"""
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
            if nonce in self.app.cache:
                raise ParseError(extra_msg="nonce param error")
            else:
                self.app.cache.add(nonce, self._nonce_timeout)
            request.body = request.body["body"]

            # set share data
            request.state.crypto = crypto

            return request
        except Exception as e:
            raise CryptoError(str(e)) from e

    async def encrypt_response(self, response: Response) -> Response:
        """encrypt response body"""
        crypto_id: str = response.header.get("crypto_id", None)
        crypto: Optional[Crypto] = self.get_crypto_by_key_id(crypto_id)
        if not crypto:
            return response
        response.body = crypto.encrypt_object(
            {"body": response.body, "timestamp": int(time.time()), "nonce": gen_random_time_id()}
        )
        return response


class CryptoProcessor(BaseCryptoProcessor):
    async def process_request(self, request: Request) -> Request:
        if request.msg_type in (Constant.MSG_REQUEST, Constant.CHANNEL_REQUEST):
            return await self.decrypt_request(request)
        return request

    async def process_response(self, response: Response) -> Response:
        if response.msg_type in (Constant.MSG_RESPONSE, Constant.CHANNEL_RESPONSE):
            return await self.encrypt_response(response)
        return response


class AutoCryptoProcessor(BaseCryptoProcessor):
    """
    Provide symmetric encryption and prevent message replay attacks;
    The auto-negotiation key of this mode is in plain text, which may be attacked
    """

    async def process_request(self, request: Request) -> Request:
        if request.msg_type == Constant.CLIENT_EVENT and request.target.endswith(Constant.DECLARE):
            check_id: str = request.body.get("check_id", "")
            crypto_id: str = request.body.get("crypto_id", "")
            crypto_key: str = request.body.get("crypto_key", "")
            if not crypto_id or not crypto_key or not check_id:
                raise CryptoError("crypto param error")
            if not self.get_crypto_by_key_id(crypto_id):
                self.load_aes_key_dict({crypto_id: crypto_key})
                request.conn.conn_future.add_done_callback(lambda f: self.del_crypto_by_key_id(crypto_id))
            request.app.cache.add(f"auto_crypto:{request.conn.conn_id}:init", 60, (crypto_id, crypto_key, check_id))

            return request
        else:
            return await super().decrypt_request(request)

    async def process_response(self, response: Response) -> Response:
        if response.msg_type == Constant.SERVER_EVENT and response.target.endswith(Constant.DECLARE):
            if not response.conn:
                raise RPCError(f"Not found conn from:{response}")
            result: Optional[Tuple[str, str, str]] = response.app.cache.get(f"auto_crypto:{response.conn.conn_id}:init")
            if not result:
                raise CryptoError("crypto id error")
            crypto_id, crypto_key, check_id = result
            crypto: Optional[Crypto] = self.get_crypto_by_key_id(crypto_id)
            if crypto is None:
                raise CryptoError("crypto id error")
            response.body = {
                "crypto_id": crypto_id,
                "crypto_key": crypto_key,
                "check_body": crypto.encrypt_object(check_id),
            }
            return response
        else:
            return await super().encrypt_response(response)
