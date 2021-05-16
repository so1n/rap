import inspect
from typing import Awaitable, Callable, List, Optional, Set, Tuple, Union

from rap.common.exceptions import TooManyRequest
from rap.common.utils import Constant
from rap.server.model import Request, Response
from rap.server.processor.base import BaseProcessor
from rap.server.processor.limit.backend import BaseLimitBackend
from rap.server.processor.limit.rule import Rule
from rap.server.processor.limit.util import RULE_FUNC_TYPE


class LimitProcessor(BaseProcessor):
    def __init__(self, backend: BaseLimitBackend, rule_list: List[Tuple[RULE_FUNC_TYPE, Rule]]):
        self._backend: BaseLimitBackend = backend
        self._rule_list: List[Tuple[RULE_FUNC_TYPE, Rule]] = rule_list
        self._ignore_request_num_set: Set = {Constant.CHANNEL_REQUEST, Constant.CLIENT_EVENT, Constant.CHANNEL_RESPONSE}

    async def process_request(self, request: Request) -> Request:
        if request.num in self._ignore_request_num_set:
            return request

        for func, rule in self._rule_list:
            if inspect.iscoroutinefunction(func):
                key: Optional[str] = await func(request)  # type: ignore
            else:
                key = func(request)
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

    async def process_response(self, response: Response) -> Response:
        return response
