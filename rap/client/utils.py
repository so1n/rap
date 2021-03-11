import inspect
from types import FunctionType
from typing import Any, Callable, Dict, List, Optional, Tuple, Type

from rap.common import exceptions as rap_exc
from rap.common.exceptions import RPCError
from rap.common.types import is_type


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


def get_func_arg_type_list(func: Callable) -> List[Type]:
    param_type_list: List[Type] = []
    var_name_list: Tuple[str, ...] = func.__code__.co_varnames
    annotation_dict: Dict[str, Type] = func.__annotations__
    for var_name in var_name_list:
        if var_name in annotation_dict:
            param_type_list.append(annotation_dict[var_name])
    return param_type_list


def check_func_type(func: Callable, param_list: Tuple[Any]) -> None:
    param_type_list: List[Type] = get_func_arg_type_list(func)
    for index, arg_type in enumerate(param_type_list):
        if not is_type(type(param_list[index]), arg_type):
            raise TypeError(f"{param_list[index]} type must: {arg_type}")
