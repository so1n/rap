import logging
import time
from dataclasses import MISSING
from typing import Any, Dict, Generator, Optional, Tuple

from .utils import get_event_loop


class Cache(object):
    """Dict with expiration time function"""

    def __init__(self, interval: Optional[float] = None) -> None:
        """
        :param interval: Scan all keys interval
        """
        self._idling_times: int = 1
        self._dict: Dict[Any, Tuple[float, Any]] = {}
        self._interval: float = interval or 10.0

    def _add(self, key: Any, expire: float, value: Any = MISSING) -> None:
        self._dict[key] = (time.time() + expire, value)

    def update_expire(self, key: Any, expire: float) -> bool:
        if key not in self._dict:
            return False
        _, value = self._dict[key]
        self._dict[key] = (expire, value)
        return True

    def get_and_update_expire(self, key: Any, expire: float, default: Any = MISSING) -> Any:
        if key not in self:
            raise KeyError(key)
        _, value = self._dict[key]
        self._dict[key] = (expire, value)
        if value is MISSING:
            if default is MISSING:
                raise KeyError(key)
            return default
        return value

    def get(self, key: Any, default: Any = MISSING) -> Any:
        if key not in self:
            raise KeyError(key)
        expire, value = self._dict[key]
        if value is MISSING:
            if default is MISSING:
                raise KeyError(key)
            return default
        return value

    def add(self, key: Any, expire: float, value: Any = MISSING) -> None:
        self._add(key, expire, value)
        if get_event_loop().is_running():
            self._auto_remove()
            setattr(self, self.add.__name__, self._add)

    def pop(self, key: Any) -> Any:
        return self._dict.pop(key, None)

    def items(self) -> Generator[Tuple[Any, Any], None, None]:
        for key in self._dict.keys():
            yield key, self._dict[key][1]

    def __contains__(self, key: Any) -> bool:
        if key not in self._dict:
            return False

        expire, value = self._dict[key]
        if expire < time.time():
            self.pop(key)
            return False
        else:
            return True

    def _auto_remove(self) -> None:
        key_list: list = list(self._dict.keys())
        index: int = 0
        if not key_list:
            self._idling_times += 1
        else:
            self._idling_times = 1
            for index, key in enumerate(key_list):
                if index > 100 and index % 100 == 0:
                    self._idling_times = 0
                    break
                if key in self:
                    # NOTE: After Python 3.7, the dict is ordered.
                    # Since the inserted data is related to time, the dict here is also ordered by time
                    break

        next_call_interval: float = self._idling_times * self._interval
        logging.debug(
            f"{self.__class__.__name__} auto remove key length:{len(key_list)}, clean cnt:{index},"
            f"until the next call:{next_call_interval}"
        )
        get_event_loop().call_later(next_call_interval, self._auto_remove)
