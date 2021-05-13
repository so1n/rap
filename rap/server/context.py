from contextvars import ContextVar
from typing import Any, Dict, get_type_hints, Type
from rap.server.model import RequestModel


rap_context: ContextVar[Dict[str, Any]] = ContextVar("rap_context", default={})


class Context:
    request: RequestModel

    def __getattr__(self, key: str) -> Any:
        value: Any = rap_context.get().get(key)
        return value

    def __setattr__(self, key: str, value: Any) -> None:
        rap_context.get()[key] = value


context: Context = Context()