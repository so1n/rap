import inspect
from typing import Awaitable, Callable, List, Optional, Set, Tuple, Union

from rap.common.exceptions import TooManyRequest
from rap.common.utlis import Constant
from rap.server.model import RequestModel, ResponseModel
from rap.server.processor.base import BaseProcessor
from rap.server.processor.limit.backend import BaseLimitBackend
from rap.server.processor.limit.rule import Rule
from rap.server.processor.limit.util import RULE_FUNC_TYPE


class LimitProcessor(BaseProcessor):
    def __init__(self, backend: BaseLimitBackend, rule_list: List[Tuple[RULE_FUNC_TYPE, Rule]]):
        self._backend: BaseLimitBackend = backend
        self._rule_list: List[Tuple[RULE_FUNC_TYPE, Rule]] = rule_list
        self._ignore_request_num_set: Set = {Constant.CHANNEL_REQUEST, Constant.CLIENT_EVENT, Constant.CHANNEL_RESPONSE}

    def register(self, func: Callable, name: Optional[str] = None, group: Optional[str] = None) -> None:
        if not group:
            group = self.__class__.__name__
        super(LimitProcessor, self).register(func, group=group)

    async def process_request(self, request: RequestModel) -> RequestModel:
        if request.num in self._ignore_request_num_set:
            return request

        key: Optional[str] = None

        for func, rule in self._rule_list:
            if inspect.iscoroutinefunction(func):
                key = await func(request)
            else:
                key = func(request)
            if key:
                break
        else:
            raise TooManyRequest()

        key = f"rap:processor:{self.__class__.__name__}:{key}"
        can_requests: Union[bool, Awaitable[bool]] = self._backend.can_requests(key, rule)
        if inspect.isawaitable(can_requests):
            can_requests = await can_requests
        if not can_requests:
            expected_time: Union[float, Awaitable[float]] = self._backend.expected_time(key, rule)
            if inspect.isawaitable(expected_time):
                expected_time = await expected_time
            raise TooManyRequest(extra_msg=f"expected time: {expected_time}")
        return request

    async def process_response(self, response: ResponseModel) -> ResponseModel:
        return response
