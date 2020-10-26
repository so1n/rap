import logging
import time
from typing import Union

from rap.common.exceptions import ServerError
from rap.manager.client_manager import ClientModel
from rap.middleware.base_middleware import BaseMsgMiddleware


class AccessMsgMiddleware(BaseMsgMiddleware):

    async def dispatch(
            self, call_id: int, method_name: str, param: str, client_model: 'ClientModel'
    ) -> Union[dict, Exception]:
        start_time: float = time.time()
        status: bool = False

        try:
            result = await self.call_next(call_id, method_name, param, client_model)
            status = True
        except Exception:
            result = ServerError('execute func error')
        if isinstance(result, Exception):
            status = False
        logging.info(f"Method:{method_name}, time:{time.time() - start_time}, status:{status}")
        return result
