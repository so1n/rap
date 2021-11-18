from typing import Callable, Optional, Tuple

from rap.server.model import Request

RULE_FUNC_RETURN_TYPE = Tuple[Optional[str], bool]
RULE_FUNC_TYPE = Callable[[Request], RULE_FUNC_RETURN_TYPE]
