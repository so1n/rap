from typing import Any, Generic, Optional, TypeVar

from typing_extensions import Self

from rap.common.event import Event
from rap.common.state import Context
from rap.common.types import MSG_TYPE
from rap.common.utils import constant

ContextTyper = TypeVar("ContextTyper", bound=Context)


class BaseMsgProtocol(Generic[ContextTyper]):
    body: Any
    header: dict
    context: ContextTyper

    def __str__(self) -> str:
        return str({k: v for k, v in self.__dict__.items() if not k.startswith("__")})

    @property
    def correlation_id(self) -> int:
        return self.context.correlation_id

    @property
    def target(self) -> str:
        return self.context.target

    @property
    def func_name(self) -> str:
        return self.context.func_name

    @property
    def group(self) -> str:
        return self.context.group

    @property
    def msg_type(self) -> int:
        return self.context.msg_type

    @correlation_id.setter
    def correlation_id(self, value: int) -> None:
        self.context.correlation_id = value

    @target.setter
    def target(self, value: str) -> None:
        self.header["target"] = value
        self.context.target = value

    @func_name.setter
    def func_name(self, value: str) -> None:
        self.context.func_name = value

    @group.setter
    def group(self, value: str) -> None:
        self.context.group = value

    @msg_type.setter
    def msg_type(self, value: int) -> None:
        self.context.msg_type = value


class BaseRequest(BaseMsgProtocol[ContextTyper]):
    def __init__(
        self,
        *,
        msg_type: int,
        target: Optional[str] = None,
        body: Any,
        context: ContextTyper,
        header: Optional[dict] = None,
    ):
        self.body: Any = body
        self.header = header or {}
        self.context: ContextTyper = context

        target = target or self.header.get("target", "")
        if target:
            _, group, func_name = target.split("/")
            self.target = target
            self.func_name = func_name
            self.group = group
            self.msg_type = msg_type

    def to_msg(self) -> MSG_TYPE:
        return self.msg_type, self.correlation_id, self.header, self.body

    @classmethod
    def from_msg(cls, msg: MSG_TYPE, context: ContextTyper) -> "Self":
        return cls(msg_type=msg[0], header=msg[2], body=msg[3], context=context)

    @classmethod
    def from_event(cls, event: Event, context: ContextTyper) -> "Self":
        request: "BaseRequest" = cls(
            msg_type=constant.CLIENT_EVENT, target=f"/_event/{event.event_name}", body=event.event_info, context=context
        )
        return request


class BaseResponse(BaseMsgProtocol[ContextTyper]):
    def __init__(
        self,
        *,
        msg_type: int,
        header: Optional[dict] = None,
        body: Any = None,
        context: ContextTyper,
    ):
        self.context: ContextTyper = context
        self.msg_type: int = msg_type
        self.body: Any = body
        self.header = header or {}

    @property
    def status_code(self) -> int:
        return self.header.get("status_code", 200)

    def to_json(self) -> dict:
        return {"msg_type": self.msg_type, "cid": self.correlation_id, "header": self.header, "body": self.body}

    def to_msg(self) -> MSG_TYPE:
        return self.msg_type, self.correlation_id, self.header, self.body

    @classmethod
    def from_msg(cls, *, msg: MSG_TYPE, context: ContextTyper) -> "Self":
        response: "BaseResponse" = cls(msg_type=msg[0], header=msg[2], body=msg[3], context=context)
        target = response.header.get("target", "")
        if target:
            _, group, func_name = target.split("/")
            response.target = target
            response.func_name = func_name
        return response
