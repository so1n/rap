from contextvars import ContextVar, Token
from typing import Any, Dict, List, Optional

from rap.server.model import Request

rap_context: ContextVar[Dict[str, Any]] = ContextVar("rap_context", default={})

_token: List[Optional[Token]] = [None]


class Context(object):
    request: Request

    def __getattr__(self, key: str) -> Any:
        value: Any = rap_context.get().get(key)
        return value

    def __setattr__(self, key: str, value: Any) -> None:
        rap_context.get()[key] = value

    def __enter__(self) -> "Context":
        _token[0] = rap_context.set({})
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        token: Optional[Token] = _token[0]
        if token:
            rap_context.reset(token)
            _token[0] = None


context: Context = Context()
