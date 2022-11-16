from typing import TYPE_CHECKING

from rap.common.msg import BaseRequest, BaseResponse
from rap.common.state import Context

if TYPE_CHECKING:
    from rap.client.transport.transport import Transport


class ClientContext(Context):
    transport: "Transport"


class Request(BaseRequest[ClientContext]):
    """rap client request obj"""


class Response(BaseResponse[ClientContext]):
    """rap server request obj"""
