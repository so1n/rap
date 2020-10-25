import logging
from typing import Union
from rap.manager.client_manager import ClientModel
from rap.middleware.base_middleware import BaseMsgMiddleware


class AccessMsgMiddleware(BaseMsgMiddleware):

    async def dispatch(
            self, call_id: int, method_name: str, param: str, client_model: 'ClientModel'
    ) -> Union[dict, Exception]:
        logging.info(f'call_id:{call_id} call method: {method_name}')
        result = await self.call_next(call_id, method_name, param, client_model)
        return result
