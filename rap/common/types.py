import asyncio
from collections.abc import AsyncIterator, Iterator
from typing import Any, List, Optional, Set, Tuple, Type, Union, _GenericAlias

import msgpack


def _f(*args, **kwargs):
    pass


# request num, msg id, group, func name, header, body
BASE_REQUEST_TYPE = Tuple[int, int, str, str, dict, Any]
BASE_RESPONSE_TYPE = Optional[Tuple[int, int, str, str, dict, Any]]
LOOP_TYPE = asyncio.get_event_loop
READER_TYPE = asyncio.streams.StreamReader
WRITER_TYPE = asyncio.streams.StreamWriter
UNPACKER_TYPE = msgpack.Unpacker

_CAN_JSON_TYPE_SET: Set[type] = {bool, dict, float, int, list, str, tuple, type(None), None}
FunctionType = type(_f)


def parse_typing(_type: Type) -> Union[List[Type], Type]:
    """
    parse typing.type to Python.type
    >>> from typing import Dict, Optional
    >>> assert dict is parse_typing(dict)
    >>> assert list is parse_typing(List)
    >>> assert dict is parse_typing(Dict)
    >>> assert dict in set(parse_typing(Optional[Dict]))
    >>> assert None in set(parse_typing(Optional[Dict]))
    >>> assert dict in set(parse_typing(Optional[dict]))
    >>> assert None in set(parse_typing(Optional[dict]))
    >>> assert dict is parse_typing(Union[dict])
    >>> assert dict is parse_typing(Union[Dict])
    >>> assert dict is parse_typing(Union[Dict[str, Any]])
    """
    if isinstance(_type, _GenericAlias):
        origin: type = _type.__origin__
        if origin is Union:
            return [parse_typing(i) for i in _type.__args__]
        elif origin in (AsyncIterator, Iterator):
            return _type.__args__[0]
        return origin
    elif _type in _CAN_JSON_TYPE_SET:
        return _type
    else:
        raise RuntimeError(f"Can not parse {_type} origin type")


def is_json_type(_type: Type) -> bool:
    """
    check type is legal json type
    >>> from typing import Dict, Optional
    >>> assert is_json_type(parse_typing(dict))
    >>> assert is_json_type(parse_typing(List))
    >>> assert is_json_type(parse_typing(Dict))
    >>> assert is_json_type(parse_typing(Optional[Dict]))
    >>> assert is_json_type(parse_typing(Optional[Dict]))
    >>> assert is_json_type(parse_typing(Optional[dict]))
    >>> assert is_json_type(parse_typing(Optional[dict]))
    >>> assert is_json_type(parse_typing(Union[dict]))
    >>> assert is_json_type(parse_typing(Union[Dict]))
    >>> assert is_json_type(parse_typing(Union[Dict[str, Any]]))
    """
    origin_type: Union[List[Type], Type] = parse_typing(_type)
    if type(origin_type) is list:
        return not bool(set(origin_type) - _CAN_JSON_TYPE_SET)
    return origin_type in _CAN_JSON_TYPE_SET


def is_type(source_type: Type, target_type: Type) -> bool:
    origin_type: Union[List[Type], Type] = parse_typing(source_type)
    if type(origin_type) is not list:
        origin_type = [origin_type]
    return target_type in origin_type
