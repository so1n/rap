from typing import Any

from typing_extensions import Protocol


class BaseMsgProtocol(Protocol):
    msg_type: int
    target: str
    body: Any
    correlation_id: int
    header: dict

    def __str__(self) -> str:
        return str({k: v for k, v in self.__dict__.items() if not k.startswith("__")})
