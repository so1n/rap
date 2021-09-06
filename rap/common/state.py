from dataclasses import MISSING
from typing import Any, Dict, Optional


class State(object):
    """copy from starlette"""

    def __init__(self, state: Optional[Dict] = None):
        if state is None:
            state = {}
        super(State, self).__setattr__("_state", state)

    def __setattr__(self, key: Any, value: Any) -> None:
        self._state[key] = value

    def __getattr__(self, key: Any) -> Any:
        try:
            return self._state[key]
        except KeyError:
            raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{key}'")

    def __len__(self) -> int:
        return len(self._state)

    def __delattr__(self, key: Any) -> None:
        del self._state[key]

    def get_value(self, key: Any, default_value: Any = MISSING) -> Any:
        try:
            return self._state[key]
        except KeyError:
            if default_value is MISSING:
                raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{key}'")
            return default_value
