import logging
import time

from rap.common.utils import Constant
from rap.server.model import Request, Response
from rap.server.plugin.processor.base import BaseProcessor


class AccessProcessor(BaseProcessor):
    async def process_request(self, request: Request) -> Request:
        host: str = request.header["host"]
        if request.num == Constant.MSG_REQUEST:
            request.stats.access_processor_start_time = time.time()
        elif (
            request.num == Constant.CHANNEL_REQUEST
            and request.header.get("channel_life_cycle", "error") == Constant.DECLARE
        ):
            logging.info(f"host:{host} declare channel. group:{request.group} func:{request.func_name}")
        return request

    async def process_response(self, response: Response) -> Response:
        host: str = response.header["host"]
        status_code: int = response.header["status_code"]
        if response.num == Constant.MSG_RESPONSE:
            logging.info(
                f"host:{host}, group:{response.group}, func:{response.func_name},"
                f" time:{time.time() - response.stats.access_processor_start_time }, status:{status_code >= 400}"
            )
        elif (
            response.num == Constant.CHANNEL_RESPONSE
            and response.header.get("channel_life_cycle", "error") == Constant.DROP
        ):
            logging.info(f"host:{host}, group:{response.group}, func:{response.func_name} drop")
        return response
