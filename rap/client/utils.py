import inspect
from typing import Dict, Optional, Type

from rap.common import exceptions as rap_exc
from rap.common.exceptions import RPCError


def get_exc_status_code_dict() -> Dict[int, Type[rap_exc.BaseRapError]]:
    exc_dict: Dict[int, Type[rap_exc.BaseRapError]] = {}
    for exc_name in dir(rap_exc):
        class_: Type = getattr(rap_exc, exc_name)
        if (
            inspect.isclass(class_)
            and issubclass(class_, rap_exc.BaseRapError)
            and class_.__name__ != rap_exc.BaseRapError.__class__.__name__
        ):
            exc_dict[class_.status_code] = class_
    return exc_dict


def raise_rap_error(exc_name: str, exc_info: str = "") -> None:
    """raise python exception"""
    exc: Optional[Type[Exception]] = getattr(rap_exc, exc_name, None)
    if exc is None:
        exc = globals()["__builtins__"].get(exc_name, None)
    if exc is None:
        raise RPCError(exc_info)
    else:
        raise exc(exc_info)
