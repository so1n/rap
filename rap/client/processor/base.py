from typing import TYPE_CHECKING, Callable, Dict, List, Tuple

from rap.client.model import Request, Response
from rap.common.utils import EventEnum
from rap.client.types import CLIENT_EVENT_FN

if TYPE_CHECKING:
    from rap.client.core import BaseClient


class BaseProcessor(object):

    app: "BaseClient"
    event_dict: Dict["EventEnum", List[CLIENT_EVENT_FN]] = {}

    async def process_request(self, request: Request) -> Request:
        return request

    async def process_response(self, response: Response) -> Response:
        return response

    async def process_exc(self, response: Response, exc: Exception) -> Tuple[Response, Exception]:
        return response, exc
