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
    def from_event(cls, server_name: str, event: Event) -> "Request":
        return cls(
            msg_type=Constant.CLIENT_EVENT,
            target=f"{server_name}/_event/{event.event_name}",
            body=event.event_info
        )


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
        return cls(conn, *msg)

    def _target_handle(self) -> None:
        if not self._target_dict:
            server_name, group, func_name = self.target.split("/")
            self._target_dict = {"server_name": server_name, "group": group, "func_name": func_name}

    @property
    def server_name(self) -> str:
        self._target_handle()
        return self._target_dict["server_name"]

    @property
    def group(self) -> str:
        self._target_handle()
        return self._target_dict["group"]

    @property
    def func_name(self) -> str:
        self._target_handle()
        return self._target_dict["func_name"]


