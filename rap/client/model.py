from dataclasses import dataclass, field
from typing import Any

from rap.common.conn import Connection
from rap.common.event import Event
from rap.common.types import BASE_MSG_TYPE, MSG_TYPE
from rap.common.utils import Constant


@dataclass()
class Request(object):
    msg_type: int
    target: str
    body: Any
    correlation_id: str = ""
    header: dict = field(default_factory=lambda: dict())

    def to_msg(self) -> MSG_TYPE:
        return self.msg_type, self.correlation_id, self.target, self.header, self.body

    @classmethod
    def from_event(cls, event: Event) -> "Request":
        return cls(msg_type=Constant.CLIENT_EVENT, target=f"/_event/{event.event_name}", body=event.event_info)


@dataclass()
class Response(object):
    conn: Connection
    msg_id: int
    msg_type: int
    correlation_id: str
    target: str
    header: dict
    body: Any

    _target_dict: dict = field(default_factory=dict)

    @classmethod
    def from_msg(cls, conn: Connection, msg: BASE_MSG_TYPE) -> "Response":
        resp: "Response" = cls(conn, *msg)
        _, group, func_name = resp.target.split("/")
        resp._target_dict = {"group": group, "func_name": func_name}
        return resp

    @property
    def group(self) -> str:
        return self._target_dict["group"]

    @property
    def func_name(self) -> str:
        return self._target_dict["func_name"]
