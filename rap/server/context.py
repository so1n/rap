from contextvars import ContextVar
from typing import Any, Dict

from rap.server.model import Request

rap_context: ContextVar[Dict[str, Any]] = ContextVar("rap_context", default={})


class Context:
    request: Request

    def __getattr__(self, key: str) -> Any:
        value: Any = rap_context.get().get(key)
        return value

    def __setattr__(self, key: str, value: Any) -> None:
        rap_context.get()[key] = value


context: Context = Context()
