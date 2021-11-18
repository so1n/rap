import logging
import time

from rap.common.utils import constant
from rap.server.model import Request, Response
from rap.server.plugin.processor.base import BaseProcessor

logger: logging.Logger = logging.getLogger(__name__)


class AccessProcessor(BaseProcessor):
    """print access log"""

    async def process_request(self, request: Request) -> Request:
        host: str = request.header["host"]
        if request.msg_type == constant.MSG_REQUEST:
            request.state.access_processor_start_time = time.time()
        elif (
            request.msg_type == constant.CHANNEL_REQUEST
            and request.header.get("channel_life_cycle", "error") == constant.DECLARE
        ):
            logger.info(f"host:{host} declare channel. group:{request.group} func:{request.func_name}")
        return request

    async def process_response(self, response: Response) -> Response:
        host: str = response.header["host"]
        status_code: int = response.header["status_code"]
        if response.msg_type == constant.MSG_RESPONSE:
            logger.info(
                f"host:{host}, target: {response.target},"
                f" time:{time.time() - response.state.access_processor_start_time }, status:{status_code >= 400}"
            )
        elif (
            response.msg_type == constant.CHANNEL_RESPONSE
            and response.header.get("channel_life_cycle", "error") == constant.DROP
        ):
            logger.info(f"host:{host}, target: {response.target} drop")
        return response
