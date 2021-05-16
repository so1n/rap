import logging
import time
from typing import Any, Callable, Dict, Tuple

from rap.common.exceptions import ServerError
from rap.server.middleware.base import BaseMsgMiddleware
from rap.server.model import Request


class AccessMsgMiddleware(BaseMsgMiddleware):
    async def dispatch(
        self, request: Request, call_id: int, func: Callable, param: list, default_param: Dict[str, Any]
    ) -> Tuple[int, Any]:
        start_time: float = time.time()
        status: bool = False

        try:
            call_id, result = await self.call_next(request, call_id, func, param, default_param)
            if not isinstance(result, Exception):
                status = True
        except Exception as e:
            logging.exception(e)
            result = ServerError("execute func error")
        logging.info(f"func:{func}, time:{time.time() - start_time}, status:{status}")
        return call_id, result
