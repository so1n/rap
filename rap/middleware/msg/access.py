import logging
import time
from typing import Callable, Tuple, Union

from rap.common.exceptions import ServerError
from rap.manager.client_manager import ClientModel
from rap.middleware.base_middleware import BaseMsgMiddleware


class AccessMsgMiddleware(BaseMsgMiddleware):
    async def dispatch(
        self, header: dict, call_id: int, method: Callable, param: str, client_model: "ClientModel"
    ) -> Tuple[int, Union[dict, Exception]]:
        start_time: float = time.time()
        status: bool = False

        try:
            call_id, result = await self.call_next(header, call_id, method, param, client_model)
            status = True
        except Exception as e:
            logging.exception(e)
            result = ServerError("execute func error")
        if isinstance(result, Exception):
            status = False
        logging.info(f"Method:{method}, time:{time.time() - start_time}, status:{status}")
        return call_id, result
