from typing import Awaitable, Callable, Optional, Union

from rap.server.model import RequestModel

RULE_FUNC_RETURN_TYPE = Optional[str]
RULE_FUNC_TYPE = Callable[[RequestModel], RULE_FUNC_RETURN_TYPE]
