import inspect
from typing import Awaitable, List, Set, Tuple, Union

from rap.common.exceptions import TooManyRequest
from rap.common.utils import constant
from rap.server.model import Request
from rap.server.plugin.processor.base import BaseProcessor
from rap.server.plugin.processor.limit.backend import BaseLimitBackend
from rap.server.plugin.processor.limit.rule import Rule
from rap.server.plugin.processor.limit.util import RULE_FUNC_TYPE


class LimitProcessor(BaseProcessor):
    def __init__(self, backend: BaseLimitBackend, rule_list: List[Tuple[RULE_FUNC_TYPE, Rule]]):
        self._backend: BaseLimitBackend = backend
        self._rule_list: List[Tuple[RULE_FUNC_TYPE, Rule]] = rule_list
        self._ignore_request_num_set: Set = {constant.CHANNEL_REQUEST, constant.CLIENT_EVENT, constant.CHANNEL_RESPONSE}

    async def process_request(self, request: Request) -> Request:
        # not limit client event
        if request.msg_type == constant.CLIENT_EVENT:
            return request

        for func, rule in self._rule_list:
            if inspect.iscoroutinefunction(func):
                key, is_ignore_limit = await func(request)  # type: ignore
            else:
                key, is_ignore_limit = func(request)
            if is_ignore_limit:
                return request
            if key:
                break
        else:
            raise TooManyRequest()

        key = f"rap:processor:{self.__class__.__name__}:{key}"
        can_requests: Union[bool, Awaitable[bool]] = self._backend.can_requests(key, rule)
        if inspect.isawaitable(can_requests):
            can_requests = await can_requests  # type: ignore
        if not can_requests:
            expected_time: Union[float, Awaitable[float]] = self._backend.expected_time(key, rule)
            if inspect.isawaitable(expected_time):
                expected_time = await expected_time  # type: ignore
            raise TooManyRequest(extra_msg=f"expected time: {expected_time}")
        return request
