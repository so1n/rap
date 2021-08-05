import logging
from dataclasses import dataclass, field
from typing import Any, Optional

from rap.common.conn import ServerConnection
from rap.common.event import Event
from rap.common.exceptions import BaseRapError, ServerError
from rap.common.state import State
from rap.common.types import BASE_MSG_TYPE, SERVER_MSG_TYPE
from rap.common.utils import Constant


@dataclass()
class Request(object):
    conn: ServerConnection
    msg_id: int
    msg_type: int
    correlation_id: str
    target: str
    header: dict
    body: Any
    stats: "State" = field(default_factory=State)

    _target_dict: dict = field(default_factory=dict)

    @classmethod
    def from_msg(cls, msg: BASE_MSG_TYPE, conn: ServerConnection) -> "Request":
        request: "Request" = cls(conn, *msg)
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
class Response(object):
    target: str
    msg_type: int = Constant.MSG_RESPONSE
    correlation_id: str = ""
    status_code: int = 200
    header: dict = field(default_factory=dict)
    body: Any = None
    stats: "State" = field(default_factory=State)
    conn: Optional[ServerConnection] = None

    def set_exception(self, exc: Exception) -> None:
        if not isinstance(exc, Exception):
            raise TypeError(f"{exc} type must Exception")
        if not isinstance(exc, BaseRapError):
            logging.error(exc)
            exc = ServerError(str(exc))
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
    def from_exc(cls, exc: Exception) -> "Response":
        response: Response = cls(f"/_exc/server_error")
        response.set_exception(exc)
        return response

    @classmethod
    def from_event(cls, event: Event) -> "Response":
        if not isinstance(event, Event):
            raise TypeError(f"event type:{event} is not {Event}")
        response: Response = cls(f"/_event/{event.event_name}")
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
