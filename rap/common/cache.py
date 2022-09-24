import asyncio
import logging
import time
from dataclasses import MISSING
from threading import Lock
from typing import Any, Dict, Generator, List, Optional, Tuple

from .asyncio_helper import get_event_loop

logger: logging.Logger = logging.getLogger(__name__)


class Cache(object):
    """Dict with expiration time function"""

    def __init__(self, interval: Optional[float] = None) -> None:
        """
        :param interval: Scan all keys interval
        """
        self._no_expire_value: float = -1.0
        self._idling_count: int = 1
        self._dict: Dict[Any, List] = {}
        self._auto_remove_handler: Optional[asyncio.TimerHandle] = None

        self._interval: float = interval or 10.0

    def _real_expire(self, expire: float) -> float:
        if expire == self._no_expire_value:
            return self._no_expire_value
        else:
            return time.time() + expire

    def _add(self, key: Any, expire: float, value: Any = None) -> None:
        self._dict[key] = [self._real_expire(expire), value]
        if self._idling_count > 1:
            if self._auto_remove_handler:
                self._auto_remove_handler.cancel()
                self._auto_remove_handler = None
            self._auto_remove()

    def update_expire(self, key: Any, expire: float) -> bool:
        """update key expire
        :param key: cache key
        :param expire: key new expire
        """
        if key not in self._dict:
            return False
        self._dict[key][0] = self._real_expire(expire)
        return True

    def _get(self, key: Any, default: Any = MISSING) -> Tuple[float, Any]:
        try:
            if key not in self:
                # Determine if the current key has expired
                raise KeyError(key)
            expire, value = self._dict[key]
        except KeyError:
            if default is MISSING:
                raise KeyError(key)
            return -1, default
        return expire, value

    def get(self, key: Any, default: Any = MISSING) -> Any:
        """get value from cache

        :param key: cache key
        :param default: The default value returned when the key corresponding value is not found.
         If the default value is not filled in, an error will be thrown when the key corresponding value is not found
        """
        _, value = self._get(key, default)
        return value

    def add(self, key: Any, expire: float, value: Any = None) -> None:
        """add value by key, if value is None, like set
        :param key: cache key
        :param expire: key expire(seconds), if expire == -1, key not expire
        :param value: cache value, if value is None, cache like set
        """
        self._add(key, expire, value)
        if get_event_loop().is_running():
            self._auto_remove()
            setattr(self, self.add.__name__, self._add)

    def pop(self, key: Any, default: Any = MISSING) -> Any:
        """delete key from cache"""
        try:
            return self._dict.pop(key)[1]
        except KeyError as e:
            if default is MISSING:
                raise e
            else:
                return default

    def items(self) -> Generator[Tuple[Any, Any], None, None]:
        """like dict items"""
        for key in self._dict.keys():
            yield key, self._dict[key][1]

    def __contains__(self, key: Any) -> bool:
        """check key in cache"""
        if key not in self._dict:
            return False

        expire, value = self._dict[key]
        if expire == self._no_expire_value:
            return True
        elif expire < time.time():
            self.pop(key, None)
            return False
        else:
            return True

    def _auto_remove(self) -> None:
        """Automatically clean up expired keys"""
        key_list: list = list(self._dict.keys())
        index: int = 0
        if not key_list:
            self._idling_count += 1
        else:
            self._idling_count = 1
            for index, key in enumerate(key_list):
                if index > 100 and index % 100 == 0:
                    # There are too many cleanups, and the next cycle needs to be executed immediately
                    self._idling_count = 0
                    break
                if key in self:
                    # NOTE: After Python 3.7, the dict is ordered.
                    # Since the inserted data is related to time, the dict here is also ordered by time
                    break

        next_call_interval: float = self._idling_count * self._interval
        logger.debug(
            f"{self.__class__.__name__} auto remove key length:{len(key_list)}, clean cnt:{index},"
            f"until the next call:{next_call_interval}"
        )
        self._auto_remove_handler = get_event_loop().call_later(next_call_interval, self._auto_remove)


class ThreadCache(Cache):
    def __init__(self, interval: Optional[float] = None) -> None:
        self._look: Lock = Lock()
        super(ThreadCache, self).__init__(interval=interval)

    def update_expire(self, key: Any, expire: float) -> bool:
        if key not in self._dict:
            return False
        with self._look:
            _, value = self._dict[key]
            self._add(key, expire, value)
        return True

    def get_and_update_expire(self, key: Any, expire: float, default: Any = MISSING) -> Any:
        with self._look:
            _, value = self._get(key, default)
            if value is not MISSING:
                self._dict[key] = [self._real_expire(expire), value]
            return value
