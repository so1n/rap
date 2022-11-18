import logging
import time

from rap.common.utils import constant
from rap.server.model import Request, Response
from rap.server.plugin.processor.base import BaseProcessor, ResponseCallable

logger: logging.Logger = logging.getLogger(__name__)


class AccessProcessor(BaseProcessor):
    """print access log"""

    async def process_request(self, request: Request) -> Request:
        host: str = request.header["host"]
        if request.msg_type == constant.MT_MSG:
            request.context.access_processor_start_time = time.time()
        elif (
            request.msg_type == constant.MT_CHANNEL
            and request.header.get("channel_life_cycle", "error") == constant.DECLARE
        ):
            logger.info(f"host:{host} declare channel. group:{request.group} func:{request.func_name}")
        return request

    async def process_response(self, response_cb: ResponseCallable) -> Response:
        response: Response = await super().process_response(response_cb)
        host: str = response.header["host"]
        status_code: int = response.header["status_code"]
        if response.msg_type == constant.MT_MSG:
            logger.info(
                f"host:{host}, target: {response.target},"
                f" time:{time.time() - response.context.access_processor_start_time }, status:{status_code >= 400}"
            )
        elif (
            response.msg_type == constant.MT_CHANNEL
            and response.header.get("channel_life_cycle", "error") == constant.DROP
        ):
            logger.info(f"host:{host}, target: {response.target} drop")
        return response
