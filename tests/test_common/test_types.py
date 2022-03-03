from typing import Any, Dict, List, Optional, Union

from rap.common.types import is_json_type, is_type, parse_typing


class TestTypes:
    def test_parse_typing(self) -> None:
        def _gen_list(_type: Any) -> List:
            if not isinstance(_type, list):
                return [_type]
            return _type

        assert dict is parse_typing(dict)
        assert list is parse_typing(List)
        assert dict is parse_typing(Dict)
        assert dict in set(_gen_list(parse_typing(Optional[Dict])))
        assert type(None) in set(_gen_list(parse_typing(Optional[Dict])))
        assert dict in set(_gen_list(parse_typing(Optional[dict])))
        assert type(None) in set(_gen_list(parse_typing(Optional[dict])))
        assert dict is parse_typing(Union[dict, list])
        assert dict is parse_typing(Union[Dict, List])
        assert dict is parse_typing(Union[Dict[str, Any], List])

    def test_is_json_type(self) -> None:
        assert is_json_type(dict)
        assert is_json_type(List)
        assert is_json_type(Dict)
        assert is_json_type(Optional[Dict])
        assert is_json_type(Optional[Dict])
        assert is_json_type(Optional[dict])
        assert is_json_type(Optional[dict])
        assert is_json_type(Union[dict, list])
        assert is_json_type(Union[Dict, List])
        assert is_json_type(Union[Dict[str, Any], List])

    def test_is_type(self) -> None:
        assert is_type(int, int)
        assert is_type(List, list)
        assert is_type(int, Optional[int])
