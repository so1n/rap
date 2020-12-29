from dataclasses import dataclass, field
from typing import Any, Optional

from rap.common.utlis import Constant, State


@dataclass()
class RequestModel(object):
    num: int
    msg_id: int
    func_name: str
    method: str
    header: dict
    body: Any
    stats: "object()" = State()


@dataclass()
class ResponseModel(object):
    num: int = Constant.MSG_RESPONSE
    msg_id: int = -1
    func_name: Optional[str] = None
    method: Optional[str] = None
    header: dict = field(default_factory=lambda: {"status_code": 200})
    body: Any = None
    stats: "object()" = State()
