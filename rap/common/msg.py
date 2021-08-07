from typing import Any

from typing_extensions import Protocol

from .state import State


class BaseMsgProtocol(Protocol):
    msg_type: int
    target: str
    body: Any
    correlation_id: str
    header: dict
    state: State
