import logging
import time
from typing import Callable, Tuple, Union

from rap.common.exceptions import ServerError
from rap.server.middleware.base import BaseMsgMiddleware
from rap.server.model import RequestModel


class AccessMsgMiddleware(BaseMsgMiddleware):
    async def dispatch(
        self, request: RequestModel, call_id: int, func: Callable, param: str
    ) -> Tuple[int, Union[dict, Exception]]:
        start_time: float = time.time()
        status: bool = False

        try:
            call_id, result = await self.call_next(request, call_id, func, param)
            if not isinstance(result, Exception):
                status = True
        except Exception as e:
            logging.exception(e)
            result = ServerError("execute func error")
        logging.info(f"func:{func}, time:{time.time() - start_time}, status:{status}")
        return call_id, result
