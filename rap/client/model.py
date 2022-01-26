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


class Request(ClientMsgProtocol):
    def __init__(
        self,
        *,
        app: "BaseClient",
        msg_type: int,
        target: str,
        body: Any,
        correlation_id: int = -1,
        conn: Optional[Connection] = None,
        header: Optional[dict] = None,
        state: Optional[State] = None,
    ):
        self.app: "BaseClient" = app
        self.msg_type: int = msg_type
        self.body: Any = body
        self.correlation_id: int = correlation_id
        self.conn = conn
        self.header = header or {}
        self.state = state or State()
        self.header["target"] = target
        self.target = target

    def to_msg(self) -> MSG_TYPE:
        return self.msg_type, self.correlation_id, self.header, self.body

    @classmethod
    def from_event(cls, app: "BaseClient", event: Event) -> "Request":
        request: "Request" = cls(
            app=app, msg_type=constant.CLIENT_EVENT, target=f"/_event/{event.event_name}", body=event.event_info
        )
        return request


class Response(BaseMsgProtocol):
    def __init__(
        self,
        app: "BaseClient",
        conn: Connection,
        msg_type: int,
        correlation_id: int,
        header: dict,
        body: Any,
        state: Optional[State] = None,
    ):
        self.app: "BaseClient" = app
        self.msg_type: int = msg_type
        self.body: Any = body
        self.correlation_id: int = correlation_id
        self.conn = conn
        self.header = header or {}
        self.state = state or State()

        self.target: str = self.header["target"]
        self.status_code: int = self.header["status_code"]
        _, self.group, self.func_name = self.target.split("/")

        self.exc: Optional[Exception] = None
        self.tb: Optional[TracebackType] = None

    @classmethod
    def from_msg(cls, app: "BaseClient", conn: Connection, msg: SERVER_BASE_MSG_TYPE) -> "Response":
        return cls(app, conn, *msg)
