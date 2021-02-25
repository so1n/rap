import logging
from dataclasses import dataclass, field
from typing import Any, Optional

from rap.common.exceptions import BaseRapError, ServerError
from rap.common.types import BASE_REQUEST_TYPE, BASE_RESPONSE_TYPE
from rap.common.utlis import Constant, Event, State


@dataclass()
class RequestModel(object):
    num: int
    msg_id: int
    group: str
    func_name: str
    header: dict
    body: Any
    stats: "State" = State()

    @classmethod
    def from_msg(cls, msg: BASE_REQUEST_TYPE) -> "RequestModel":
        return cls(*msg)


@dataclass()
class ResponseModel(object):
    num: int = Constant.MSG_RESPONSE
    msg_id: int = -1
    group: str = "default"
    func_name: str = ""
    header: dict = field(default_factory=lambda: {"status_code": 200})
    body: Any = None
    stats: "State" = State()

    def set_exception(self, exc: Exception) -> None:
        if isinstance(exc, Exception) and not isinstance(exc, BaseRapError):
            logging.error(exc)
            exc = ServerError(str(exc))
        self.body = str(exc)
        self.header["status_code"] = exc.status_code

    def set_event(self, event: Event) -> None:
        if not isinstance(event, Event):
            raise TypeError(f"{event} type must {Event.__name__}")
        self.num = Constant.SERVER_EVENT
        self.func_name = event.event_name
        self.body = event.event_info

    def set_body(self, body: Any) -> None:
        self.body = body

    @classmethod
    def from_exc(cls, exc: Exception) -> "ResponseModel":
        if isinstance(exc, Exception) and not isinstance(exc, BaseRapError):
            logging.error(exc)
            exc = ServerError(str(exc))
        else:
            raise TypeError(f"{exc} type must {Exception.__name__}")
        response: "ResponseModel" = cls(body=str(exc))
        response.header["status_code"] = exc.status_code
        return response

    @classmethod
    def from_event(cls, event: Event) -> "ResponseModel":
        if not isinstance(event, Event):
            raise TypeError(f"{event} type must {Event.__name__}")
        return cls(num=Constant.SERVER_EVENT, func_name=event.event_name, body=event.event_info)

    def to_msg(self) -> BASE_RESPONSE_TYPE:
        return self.num, self.msg_id, self.group, self.func_name, self.header, self.body

    def __call__(self, content: Any) -> None:
        if isinstance(content, Exception):
            self.set_exception(content)
        elif isinstance(content, Event):
            self.set_event(content)
        else:
            self.set_body(content)
