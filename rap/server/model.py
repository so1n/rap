import logging
import sys
from dataclasses import dataclass, field
from types import TracebackType
from typing import TYPE_CHECKING, Any, Optional

from rap.common.conn import ServerConnection
from rap.common.event import Event
from rap.common.exceptions import BaseRapError, ServerError
from rap.common.msg import BaseMsgProtocol
from rap.common.state import State
from rap.common.types import BASE_MSG_TYPE, SERVER_MSG_TYPE
from rap.common.utils import Constant

if TYPE_CHECKING:
    from rap.server.core import Server


logger: logging.Logger = logging.getLogger(__name__)


class ServerMsgProtocol(BaseMsgProtocol):
    app: "Server"


@dataclass()
class Request(ServerMsgProtocol):
    app: "Server"
    conn: ServerConnection
    msg_id: int
    msg_type: int
    correlation_id: str
    target: str
    header: dict
    body: Any
    state: "State" = field(default_factory=State)

    _target_dict: dict = field(default_factory=dict)

    @classmethod
    def from_msg(cls, app: "Server", msg: BASE_MSG_TYPE, conn: ServerConnection) -> "Request":
        request: "Request" = cls(app, conn, *msg)
        _, group, func_name = request.target.split("/")
        request._target_dict = {"group": group, "func_name": func_name}
        return request

    @property
    def group(self) -> str:
        return self._target_dict["group"]

    @property
    def func_name(self) -> str:
        return self._target_dict["func_name"]


@dataclass()
class Response(ServerMsgProtocol):
    app: "Server"
    target: str
    msg_type: int = Constant.MSG_RESPONSE
    correlation_id: str = ""
    status_code: int = 200
    header: dict = field(default_factory=dict)
    body: Any = None
    state: "State" = field(default_factory=State)
    conn: Optional[ServerConnection] = None
    exc: Optional[Exception] = None
    tb: Optional[TracebackType] = None

    def set_exception(self, exc: Exception) -> None:
        if not isinstance(exc, Exception):
            raise TypeError(f"{exc} type must Exception")
        self.tb = sys.exc_info()[2]
        if not isinstance(exc, BaseRapError):
            logger.error(exc)
            exc = ServerError(str(exc))
        self.exc = exc
        self.body = str(exc)
        self.status_code = exc.status_code

    def set_event(self, event: Event) -> None:
        if not isinstance(event, Event):
            raise TypeError(f"{event} type must {Event.__name__}")
        self.msg_type = Constant.SERVER_EVENT
        self.target = f"/_event/{event.event_name}"
        self.body = event.event_info

    def set_body(self, body: Any) -> None:
        self.body = body

    @classmethod
    def from_exc(cls, app: "Server", exc: Exception) -> "Response":
        response: Response = cls(app, "/_exc/server_error")
        response.set_exception(exc)
        return response

    @classmethod
    def from_event(cls, app: "Server", event: Event) -> "Response":
        if not isinstance(event, Event):
            raise TypeError(f"event type:{event} is not {Event}")
        response: Response = cls(app, f"/_event/{event.event_name}")
        response.set_event(event)
        return response

    def to_msg(self) -> SERVER_MSG_TYPE:
        return self.msg_type, self.correlation_id, self.target, self.status_code, self.header, self.body

    def __call__(self, content: Any) -> None:
        if isinstance(content, Exception):
            self.set_exception(content)
        elif isinstance(content, Event):
            self.set_event(content)
        else:
            self.set_body(content)
