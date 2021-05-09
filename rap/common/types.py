import asyncio
from collections.abc import AsyncIterator, Iterator
from typing import Any, List, Optional, Set, Tuple, Type, Union, _GenericAlias  # type: ignore

import msgpack  # type: ignore


def _f(*args: Any, **kwargs: Any) -> None:
    pass


# request num, msg id, group, func name, header, body
BASE_REQUEST_TYPE = Tuple[int, int, str, str, dict, Any]
BASE_RESPONSE_TYPE = Tuple[int, int, str, str, dict, Any]
LOOP_TYPE = asyncio.get_event_loop
READER_TYPE = asyncio.streams.StreamReader
WRITER_TYPE = asyncio.streams.StreamWriter
UNPACKER_TYPE = msgpack.Unpacker

_CAN_JSON_TYPE_SET: Set[Optional[type]] = {bool, dict, float, int, list, str, tuple, type(None), None}
MyFunctionType = type(_f)


class ParseTypeError(Exception):
    pass


def parse_typing(_type: Type) -> Union[List[Type[Any]], Type]:
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
            type_list: List[Type[Any]] = []
            for i in _type.__args__:
                if isinstance(i, list):
                    for j in i:
                        value: Union[List[Type[Any]], Type] = parse_typing(j)
                        if isinstance(value, list):
                            type_list.extend(value)
                        else:
                            type_list.append(value)
                else:
                    value = parse_typing(i)
                    if isinstance(value, list):
                        type_list.extend(value)
                    else:
                        type_list.append(value)
            return type_list
        elif origin in (AsyncIterator, Iterator):
            return _type.__args__[0]
        return origin
    elif _type in _CAN_JSON_TYPE_SET:
        return _type
    else:
        raise ParseTypeError(f"Can not parse {_type} origin type")


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
    try:
        origin_type: Union[List[Type], Type] = parse_typing(_type)

        if isinstance(origin_type, list):
            return not bool(set(origin_type) - _CAN_JSON_TYPE_SET)
        return origin_type in _CAN_JSON_TYPE_SET
    except ParseTypeError:
        return False


def is_type(source_type: Type, target_type: Type) -> bool:
    parse_source_type: Union[List[Type], Type] = parse_typing(source_type)
    if not isinstance(parse_source_type, list):
        parse_source_type = [parse_source_type]

    parse_target_type: Union[List[Type], Type] = parse_typing(target_type)
    if not isinstance(parse_target_type, list):
        parse_target_type = [parse_target_type]
    return bool(set(parse_target_type) & set(parse_source_type))
