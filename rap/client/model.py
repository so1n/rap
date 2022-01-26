from dataclasses import dataclass, field
from types import TracebackType
from typing import TYPE_CHECKING, Any, Optional

from rap.common.conn import Connection
from rap.common.event import Event
from rap.common.msg import BaseMsgProtocol
from rap.common.state import State
from rap.common.types import MSG_TYPE, SERVER_BASE_MSG_TYPE
from rap.common.utils import constant

if TYPE_CHECKING:
    from rap.client.core import BaseClient


class ClientMsgProtocol(BaseMsgProtocol):
    app: "BaseClient"
    conn: Optional[Connection]


@dataclass()
class Request(ClientMsgProtocol):
    app: "BaseClient"
    msg_type: int
    target: str
    body: Any
    correlation_id: int = -1
    conn: Optional[Connection] = None
    header: dict = field(default_factory=lambda: dict())

    state: State = field(default_factory=State)

    def to_msg(self) -> MSG_TYPE:
        return self.msg_type, self.correlation_id, self.target, self.header, self.body

    @classmethod
    def from_event(cls, app: "BaseClient", event: Event) -> "Request":
        return cls(app, msg_type=constant.CLIENT_EVENT, target=f"/_event/{event.event_name}", body=event.event_info)


@dataclass()
class Response(BaseMsgProtocol):
    app: "BaseClient"
    conn: Connection
    msg_type: int
    correlation_id: int
    target: str
    status_code: int
    header: dict
    body: Any

    state: State = field(default_factory=State)
    _target_dict: dict = field(default_factory=dict)

    exc: Optional[Exception] = None
    tb: Optional[TracebackType] = None

    @classmethod
    def from_msg(cls, app: "BaseClient", conn: Connection, msg: SERVER_BASE_MSG_TYPE) -> "Response":
        resp: "Response" = cls(app, conn, *msg)
        _, group, func_name = resp.target.split("/")
        resp._target_dict = {"group": group, "func_name": func_name}
        return resp

    @property
    def group(self) -> str:
        return self._target_dict["group"]

    @property
    def func_name(self) -> str:
        return self._target_dict["func_name"]
