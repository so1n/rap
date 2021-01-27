from dataclasses import dataclass, field
from typing import Any, Optional

from rap.common.utlis import Constant, State
from rap.common.types import BASE_REQUEST_TYPE


@dataclass()
class RequestModel(object):
    num: int
    msg_id: int
    group: str
    func_name: str
    header: dict
    body: Any
    stats: "object()" = State()

    @classmethod
    def from_msg(cls, msg: BASE_REQUEST_TYPE) -> "RequestModel":
        return cls(*msg)


@dataclass()
class ResponseModel(object):
    num: int = Constant.MSG_RESPONSE
    msg_id: int = -1
    group: str = None
    func_name: Optional[str] = None
    header: dict = field(default_factory=lambda: {"status_code": 200})
    body: Any = None
    stats: "object()" = State()
