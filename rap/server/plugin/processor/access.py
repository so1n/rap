import logging
import time

from rap.common.utils import Constant
from rap.server.model import Request, Response
from rap.server.plugin.processor.base import BaseProcessor


class AccessProcessor(BaseProcessor):
    """print access log"""

    async def process_request(self, request: Request) -> Request:
        host: str = request.header["host"]
        if request.msg_type == Constant.MSG_REQUEST:
            request.state.access_processor_start_time = time.time()
        elif (
            request.msg_type == Constant.CHANNEL_REQUEST
            and request.header.get("channel_life_cycle", "error") == Constant.DECLARE
        ):
            logging.info(f"host:{host} declare channel. group:{request.group} func:{request.func_name}")
        return request

    async def process_response(self, response: Response) -> Response:
        host: str = response.header["host"]
        status_code: int = response.header["status_code"]
        if response.msg_type == Constant.MSG_RESPONSE:
            logging.info(
                f"host:{host}, target: {response.target},"
                f" time:{time.time() - response.state.access_processor_start_time }, status:{status_code >= 400}"
            )
        elif (
            response.msg_type == Constant.CHANNEL_RESPONSE
            and response.header.get("channel_life_cycle", "error") == Constant.DROP
        ):
            logging.info(f"host:{host}, target: {response.target} drop")
        return response
