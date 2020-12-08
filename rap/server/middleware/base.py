from abc import ABC
from typing import Callable, Dict, Optional, Union

from rap.common.conn import ServerConnection
from rap.common.middleware import BaseMiddleware as _BaseMiddleware
from rap.common.types import BASE_RESPONSE_TYPE
from rap.common.utlis import Constant
from rap.manager.func_manager import func_manager
from rap.server.requests import RequestModel
from rap.server.response import ResponseModel


class BaseMiddleware(_BaseMiddleware, ABC):
    @staticmethod
    def register(func: Callable, is_root: bool = True, group: str = "middleware"):
        func_manager.register(func, is_root=is_root, group=group)


class BaseConnMiddleware(BaseMiddleware):
    async def dispatch(self, conn: ServerConnection):
        raise NotImplementedError


class BaseRequestMiddleware(BaseMiddleware):
    response_num_dict: Dict[int, int] = {
        Constant.DECLARE_REQUEST: Constant.DECLARE_RESPONSE,
        Constant.MSG_REQUEST: Constant.MSG_RESPONSE,
        Constant.DROP_REQUEST: Constant.DROP_RESPONSE,
    }

    async def dispatch(self, request: RequestModel, response: ResponseModel) -> Optional[ResponseModel]:
        raise NotImplementedError


class BaseMsgMiddleware(BaseMiddleware):
    async def dispatch(self, request: RequestModel, call_id: int, func: Callable, param: str) -> Union[dict, Exception]:
        raise NotImplementedError


class BaseResponseMiddleware(BaseMiddleware):
    async def dispatch(self, resp: ResponseModel) -> BASE_RESPONSE_TYPE:
        raise NotImplementedError
