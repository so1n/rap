import asyncio
import msgpack
from collections.abc import Iterator

from typing import Any, List, Tuple, Type, Set, Union, _GenericAlias


BASE_REQUEST_TYPE = Tuple[int, int, dict, Any]
BASE_RESPONSE_TYPE = Tuple[int, int, dict, Any]
LOOP_TYPE = asyncio.get_event_loop
READER_TYPE = asyncio.streams.StreamReader
WRITER_TYPE = asyncio.streams.StreamWriter
UNPACKER_TYPE = msgpack.Unpacker

_CAN_JSON_TYPE_SET: Set[type] = {bool, dict, float, int, list, str, tuple, type(None)}


def parse_typing(_type: Type) -> Union[List[Type], Type]:
    """
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
        elif origin == Iterator:
            return _type.__args__[0]
        return origin
    elif _type in _CAN_JSON_TYPE_SET:
        return _type
    else:
        raise RuntimeError(f"Can not parse {_type} origin type")


def check_is_json_type(_type: Type) -> bool:
    """
    >>> from typing import Dict, Optional
    >>> assert check_is_json_type(parse_typing(dict))
    >>> assert check_is_json_type(parse_typing(List))
    >>> assert check_is_json_type(parse_typing(Dict))
    >>> assert check_is_json_type(parse_typing(Optional[Dict]))
    >>> assert check_is_json_type(parse_typing(Optional[Dict]))
    >>> assert check_is_json_type(parse_typing(Optional[dict]))
    >>> assert check_is_json_type(parse_typing(Optional[dict]))
    >>> assert check_is_json_type(parse_typing(Union[dict]))
    >>> assert check_is_json_type(parse_typing(Union[Dict]))
    >>> assert check_is_json_type(parse_typing(Union[Dict[str, Any]]))
    """
    origin_type: Union[List[Type], Type] = parse_typing(_type)
    if type(origin_type) is list:
        return not bool(set(origin_type) - _CAN_JSON_TYPE_SET)
    return origin_type in _CAN_JSON_TYPE_SET
