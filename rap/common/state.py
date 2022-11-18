from dataclasses import MISSING
from typing import Any, Dict, Optional


class State(object):
    """inherit from starlette"""

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


class Context(State):
    # assign at initialization time
    correlation_id: int
    client_info: Dict[str, Any]
    server_info: Dict[str, Any]
    # Assignment in the Request object initialization time
    target: str
    func_name: str
    group: str
    msg_type: int

    transport_metadata: dict
    channel_metadata: dict

    def __getattr__(self, key: Any) -> Any:
        try:
            return self._state[key]
        except KeyError:
            if not hasattr(self, "correlation_id"):
                raise AttributeError(f"'{self}' object has no attribute '{key}'")
            raise AttributeError(f"'{self.__class__.__name__}<{self.correlation_id}>' object has no attribute '{key}'")

    def get_value(self, key: Any, default_value: Any = MISSING) -> Any:
        try:
            return self._state[key]
        except KeyError:
            if default_value is MISSING:
                raise AttributeError(
                    f"'{self.__class__.__name__}<{self.correlation_id}>' object has no attribute '{key}'"
                )
            return default_value

    def __repr__(self) -> str:
        correlation_id: int = -1
        if hasattr(self, "correlation_id"):
            correlation_id = self.correlation_id
        return f"{self.__class__.__name__}<{correlation_id}>"
