import logging
import sys
from types import TracebackType
from typing import TYPE_CHECKING, Any, Optional

from rap.common.conn import ServerConnection
from rap.common.event import Event
from rap.common.exceptions import BaseRapError, ServerError
from rap.common.msg import BaseMsgProtocol
from rap.common.state import Context
from rap.common.types import BASE_MSG_TYPE, SERVER_MSG_TYPE
from rap.common.utils import constant

if TYPE_CHECKING:
    from rap.server.core import Server


logger: logging.Logger = logging.getLogger(__name__)


class ServerContext(Context):
    app: "Server"
    conn: ServerConnection


class ServerMsgProtocol(BaseMsgProtocol):
    context: ServerContext


class Request(ServerMsgProtocol):
    def __init__(
        self,
        msg_type: int,
        correlation_id: int,
        header: dict,
        body: Any,
        context: ServerContext,
    ):
        assert correlation_id == context.correlation_id, "correlation_id error"
        self.msg_type: int = msg_type
        self.body: Any = body
        self.correlation_id: int = correlation_id
        self.header = header or {}
        self.context: ServerContext = context

        self.target: str = self.header.get("target", "")
        state_target: Optional[str] = self.context.get_value("target", None)
        if self.target and not state_target:
            self.context.target = self.target
        elif state_target:
            self.target = state_target
        else:
            raise ValueError(f"Can not found target from {correlation_id} request")

        _, group, func_name = self.target.split("/")
        self.group: str = group
        self.func_name: str = func_name

    @classmethod
    def from_msg(cls, msg: BASE_MSG_TYPE, context: ServerContext) -> "Request":
        return cls(*msg, context=context)


class Response(ServerMsgProtocol):
    def __init__(
        self,
        *,
        context: ServerContext,
        target: Optional[str] = None,
        msg_type: int = constant.MSG_RESPONSE,
        header: Optional[dict] = None,
        body: Any = None,
        exc: Optional[Exception] = None,
        tb: Optional[TracebackType] = None,
    ):
        self.msg_type: int = msg_type
        self.body: Any = body
        self.header = header or {}
        if "status_code" not in self.header:
            self.header["status_code"] = 200
        self.context: ServerContext = context
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
        self.set_event(event)
        self.msg_type = constant.SERVER_EVENT
        self.target = f"/_event/{event.event_name}"

    def set_body(self, body: Any) -> None:
        self.body = body

    @classmethod
    def from_exc(cls, exc: Exception, context: ServerContext) -> "Response":
        response: Response = cls(context=context, target="/_exc/server_error", msg_type=constant.SERVER_ERROR_RESPONSE)
        response.set_exception(exc)
        return response

    @classmethod
    def from_event(cls, event: Event, context: ServerContext) -> "Response":
        if not isinstance(event, Event):
            raise TypeError(f"event type:{event} is not {Event}")
        response: Response = cls(context=context)
        response.set_server_event(event)
        return response

    @property
    def status_code(self) -> int:
        return self.header.get("status_code", 0)

    @status_code.setter
    def status_code(self, value: int) -> None:
        self.header["status_code"] = value

    @property  # type: ignore
    def correlation_id(self) -> int:  # type: ignore
        return self.context.correlation_id

    @property  # type: ignore
    def target(self) -> str:  # type: ignore
        return self.header.get("target", None) or self.context.target

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
