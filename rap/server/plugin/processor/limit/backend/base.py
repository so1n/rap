from typing import Any, Coroutine, Union

from rap.server.plugin.processor.limit.rule import Rule


class BaseLimitBackend(object):
    def can_requests(self, key: str, rule: Rule, token_num: int = 1) -> Union[bool, Coroutine[Any, Any, bool]]:
        raise NotImplementedError

    def expected_time(self, key: str, rule: Rule) -> Union[float, Coroutine[Any, Any, float]]:
        raise NotImplementedError
