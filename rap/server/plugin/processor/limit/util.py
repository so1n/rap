from typing import Callable, Optional

from rap.server.model import Request

RULE_FUNC_RETURN_TYPE = Optional[str]
RULE_FUNC_TYPE = Callable[[Request], RULE_FUNC_RETURN_TYPE]
