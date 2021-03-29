from dataclasses import dataclass, field
from typing import Any

from rap.common.types import BASE_REQUEST_TYPE, BASE_RESPONSE_TYPE
from rap.common.utils import Constant, Event


@dataclass()
class Request(object):
    num: int
    func_name: str
    body: Any
    group: str = ""
    header: dict = field(default_factory=lambda: dict())

    def gen_request_msg(self, msg_id: int) -> BASE_REQUEST_TYPE:
        return self.num, msg_id, self.group, self.func_name, self.header, self.body

    @classmethod
    def from_event(cls, event: Event) -> "Request":
        return cls(num=Constant.CLIENT_EVENT, func_name=event.event_name, body=event.event_info)


@dataclass()
class Response(object):
    num: int
    msg_id: int
    group: str
    func_name: str
    header: dict
    body: Any

    @classmethod
    def from_msg(cls, msg: BASE_RESPONSE_TYPE) -> "Response":
        return cls(*msg)
