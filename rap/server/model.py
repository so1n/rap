import logging
import sys
from types import TracebackType
from typing import TYPE_CHECKING, Any, Optional

from rap.common.conn import ServerConnection
from rap.common.event import Event
from rap.common.exceptions import BaseRapError, ServerError
from rap.common.msg import BaseMsgProtocol
from rap.common.state import State
from rap.common.types import BASE_MSG_TYPE, SERVER_MSG_TYPE
from rap.common.utils import constant

if TYPE_CHECKING:
    from rap.server.core import Server


logger: logging.Logger = logging.getLogger(__name__)


class ServerMsgProtocol(BaseMsgProtocol):
    app: "Server"


class Request(ServerMsgProtocol):
    def __init__(
        self,
        app: "Server",
        conn: ServerConnection,
        msg_type: int,
        correlation_id: int,
        header: dict,
        body: Any,
        state: Optional[State] = None,
    ):
        self.app: "Server" = app
        self.msg_type: int = msg_type
        self.body: Any = body
        self.correlation_id: int = correlation_id
        self.conn = conn
        self.header = header or {}
        self.state = state or State()

        self.target: str = self.header.get("target", "")
        state_target: Optional[str] = self.state.get_value("target", None)
        if self.target and not state_target:
            self.state.target = self.target
        elif state_target:
            self.target = state_target
        else:
            raise ValueError(f"Can not found target from {correlation_id} request")

        _, group, func_name = self.target.split("/")
        self.group: str = group
        self.func_name: str = func_name

    @classmethod
    def from_msg(
        cls, app: "Server", msg: BASE_MSG_TYPE, conn: ServerConnection, state: Optional[State] = None
    ) -> "Request":
        return cls(app, conn, *msg, state=state)


class Response(ServerMsgProtocol):
    def __init__(
        self,
        *,
        app: "Server",
        target: Optional[str] = None,
        msg_type: int = constant.MSG_RESPONSE,
        correlation_id: int = -1,
        header: Optional[dict] = None,
        body: Any = None,
        state: Optional[State] = None,
        conn: Optional[ServerConnection] = None,
        exc: Optional[Exception] = None,
        tb: Optional[TracebackType] = None,
    ):
        self.app: "Server" = app
        self.msg_type: int = msg_type
        self.body: Any = body
        self.correlation_id: int = correlation_id
        self.conn = conn
        self.header = header or {}
        if "status_code" not in self.header:
            self.header["status_code"] = 200
        self.state = state or State()
        if target:
            self.target = target
        self.exc: Optional[Exception] = exc
        self.tb: Optional[TracebackType] = tb

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
        self.body = event.event_info

    def set_server_event(self, event: Event) -> None:
        if not isinstance(event, Event):
            raise TypeError(f"{event} type must {Event.__name__}")
        self.msg_type = constant.SERVER_EVENT
        self.target = f"/_event/{event.event_name}"
        self.body = event.event_info

    def set_body(self, body: Any) -> None:
        self.body = body

    @classmethod
    def from_exc(cls, app: "Server", exc: Exception) -> "Response":
        response: Response = cls(app=app, target="/_exc/server_error")
        response.set_exception(exc)
        return response

    @classmethod
    def from_event(cls, app: "Server", event: Event) -> "Response":
        if not isinstance(event, Event):
            raise TypeError(f"event type:{event} is not {Event}")
        response: Response = cls(app=app)
        response.set_server_event(event)
        return response

    @property
    def status_code(self) -> int:
        return self.header.get("status_code", 0)

    @status_code.setter
    def status_code(self, value: int) -> None:
        self.header["status_code"] = value

    @property  # type: ignore
    def target(self) -> str:  # type: ignore
        return self.header.get("target", None) or self.state.target

    @target.setter
    def target(self, value: str) -> None:
        self.header["target"] = value

    def to_msg(self) -> SERVER_MSG_TYPE:
        return self.msg_type, self.correlation_id, self.header, self.body

    def __call__(self, content: Any) -> None:
        if isinstance(content, Exception):
            self.set_exception(content)
        elif isinstance(content, Event):
            self.set_server_event(content)
        else:
            self.set_body(content)
