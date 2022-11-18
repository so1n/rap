import logging
from typing import TYPE_CHECKING, Any

from rap.common.conn import ServerConnection
from rap.common.event import Event
from rap.common.exceptions import BaseRapError, ServerError
from rap.common.msg import BaseRequest, BaseResponse
from rap.common.state import Context
from rap.common.utils import constant

if TYPE_CHECKING:
    from rap.server.core import Server


logger: logging.Logger = logging.getLogger(__name__)


class ServerContext(Context):
    app: "Server"
    conn: ServerConnection


class Request(BaseRequest[ServerContext]):
    """rap server request obj"""


class Response(BaseResponse[ServerContext]):
    def set_exception(self, exc: Exception) -> None:
        assert isinstance(exc, Exception), f"{exc} type must {Exception.__name__}"

        if not isinstance(exc, BaseRapError):
            logger.error(exc)
            exc = ServerError(str(exc))
        self.body = exc.body
        self.status_code = exc.status_code

    def set_event(self, event: Event) -> None:
        assert isinstance(event, Event), f"{event} type must {Event.__name__}"

        self.body = event.event_info

    def set_server_event(self, event: Event) -> None:
        self.set_event(event)
        self.msg_type = constant.MT_SERVER_EVENT
        self.target = f"/_event/{event.event_name}"

    def set_body(self, body: Any) -> None:
        self.body = body

    @classmethod
    def from_event(cls, event: Event, context: ServerContext) -> "Response":
        assert isinstance(event, Event), f"{event} type must {Event.__name__}"
        response: Response = cls(context=context)
        response.msg_type = constant.MT_SERVER_EVENT
        response.set_server_event(event)
        return response

    def __call__(self, content: Any) -> None:
        if isinstance(content, Exception):
            self.set_exception(content)
        elif isinstance(content, Event):
            self.set_server_event(content)
        else:
            self.set_body(content)
