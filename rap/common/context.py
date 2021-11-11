from contextvars import ContextVar, Token
from typing import Any, Dict, Optional

rap_context: ContextVar[Dict[str, Any]] = ContextVar("rap_context", default={})


class Context(object):
    def __getattr__(self, key: str) -> Any:
        value: Any = rap_context.get().get(key)
        return value

    def __setattr__(self, key: str, value: Any) -> None:
        rap_context.get()[key] = value


class WithContext(Context):
    def __init__(self) -> None:
        self._token: Optional[Token] = None

    def __enter__(self) -> "WithContext":
        self._token = rap_context.set({})
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if self._token:
            rap_context.reset(self._token)
