from dataclasses import dataclass, field
from typing import Any

from rap.common.types import BASE_REQUEST_TYPE


@dataclass()
class Request(object):
    num: int
    func_name: str
    method: str
    body: Any
    header: dict = field(default_factory=lambda: dict())

    def gen_request_msg(self, msg_id: int) -> BASE_REQUEST_TYPE:
        return self.num, msg_id, self.func_name, self.method, self.header, self.body


@dataclass()
class Response(object):
    num: int
    msg_id: int
    func_name: str
    method: str
    header: dict
    body: Any
