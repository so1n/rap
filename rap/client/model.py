from dataclasses import dataclass, field
from typing import Any

from rap.common.conn import Connection
from rap.common.event import Event
from rap.common.types import BASE_MSG_TYPE, MSG_TYPE
from rap.common.utils import Constant


@dataclass()
class Request(object):
    num: int
    func_name: str
    body: Any
    group: str = ""
    header: dict = field(default_factory=lambda: dict())

    def to_msg(self) -> MSG_TYPE:
        return self.num, self.group, self.func_name, self.header, self.body

    @classmethod
    def from_event(cls, event: Event) -> "Request":
        return cls(num=Constant.CLIENT_EVENT, func_name=event.event_name, body=event.event_info)


@dataclass()
class Response(object):
    conn: Connection
    msg_id: int
    num: int
    group: str
    func_name: str
    header: dict
    body: Any

    @classmethod
    def from_msg(cls, conn: Connection, msg: BASE_MSG_TYPE) -> "Response":
        return cls(conn, *msg)
