import logging
import random
import time
from typing import Optional

from rap.client.model import Request, Response
from rap.client.processor.base import BaseClientProcessor, ResponseCallable
from rap.common.crypto import Crypto
from rap.common.exceptions import CryptoError
from rap.common.snowflake import async_get_snowflake_id
from rap.common.utils import constant, gen_random_time_id

logger: logging.Logger = logging.getLogger(__name__)


class BaseCryptoProcessor(BaseClientProcessor):
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
    """
    Provide symmetric encryption and prevent message replay attacks;
    The auto-negotiation key of this mode is in plain text, which may be attacked
    """

    def __init__(self, nonce_timeout: Optional[int] = None):
        """
        nonce_time: Cache nonce time, each message has a nonce field, and the value of each message is different,
            which is used to prevent message re-attack.
        """
        self._nonce_timeout: int = nonce_timeout or BaseCryptoProcessor._nonce_timeout

    async def process_request(self, request: Request) -> Request:
        assert request.context.transport is not None, "Not found transport from request"
        if request.msg_type == constant.MT_CLIENT_EVENT and request.target.endswith(constant.DECLARE):
            crypto_key: str = gen_random_time_id(length=6, time_length=10)
            crypto_id: str = str(await async_get_snowflake_id())
            request.body["crypto_id"] = crypto_id
            request.body["crypto_key"] = crypto_key
            request.context.transport.state.crypto = Crypto(crypto_key)
            check_id: int = random.randint(0, 999999)
            request.body["check_id"] = request.context.transport.state.crypto.encrypt_object(check_id)
            request.context.check_id = check_id
            # self.app.cache.add(crypto_id, 10, check_id)
        elif request.msg_type in (constant.MT_MSG, constant.MT_CHANNEL):
            try:
                crypto: Crypto = request.context.transport.state.crypto
                request.body = {
                    "body": request.body,
                    "timestamp": int(time.time()),
                    "nonce": await async_get_snowflake_id(),
                }
                request.body = crypto.encrypt_object(request.body)
            except Exception as e:
                raise CryptoError("Can't encrypt body.") from e
        return await super().process_request(request)

    async def process_response(self, response_cb: ResponseCallable) -> Response:
        response: Response = await super().process_response(response_cb)
        crypto: Crypto = response.context.conn.state.crypto
        try:
            if response.msg_type == constant.MT_SERVER_EVENT and response.target.endswith(constant.DECLARE):
                if crypto.decrypt_object(response.body["check_id"]) - 1 != response.context.state.check_id:
                    raise CryptoError("Check body error")
            elif response.msg_type in (constant.MT_MSG, constant.MT_CHANNEL) and response.status_code < 400:
                response.body = crypto.decrypt_object(response.body)
                self._body_handle(response.body)
                response.body = response.body["body"]
        except Exception as e:
            raise CryptoError("Can't decrypt body.") from e
        return response


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
        if request.msg_type == constant.MT_CLIENT_EVENT and request.target.endswith(constant.DECLARE):
            # Tell the server that the key will be used for encrypted communication
            request.body["crypto_id"] = self._crypto_id
            request.body["check_body"] = self._crypto.encrypt_object(self._crypto_id)
        elif request.msg_type in (constant.MT_MSG, constant.MT_CHANNEL):
            request.body = {
                "body": request.body,
                "timestamp": int(time.time()),
                "nonce": await async_get_snowflake_id(),
            }
            request.body = self._crypto.encrypt_object(request.body)
        return await super().process_request(request)

    async def process_response(self, response_cb: ResponseCallable) -> Response:
        response: Response = await super().process_response(response_cb)
        if response.msg_type in (constant.MT_MSG, constant.MT_CHANNEL) and response.status_code < 400:
            try:
                response.body = self._crypto.decrypt_object(response.body)
                self._body_handle(response.body)
                response.body = response.body["body"]
            except Exception as e:
                logger.exception(f"decrypt error:{e}")
                raise CryptoError("Can't decrypt body.") from e
        return response
