from dataclasses import dataclass, field
from typing import Any


@dataclass()
class Request(object):
    num: int
    body: Any
    header: dict = field(default_factory=lambda: dict())


@dataclass()
class Response(object):
    num: int
    msg_id: int
    header: dict
    body: Any
