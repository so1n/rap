from typing import TYPE_CHECKING, Dict, List

from rap.client.model import Request, Response
from rap.client.types import CLIENT_EVENT_FN
from rap.common.utils import EventEnum

if TYPE_CHECKING:
    from rap.client.core import BaseClient


class BaseProcessor(object):
    """client processor
    Note:
        It needs to be loaded before the client is started.
        The client will automatically register the corresponding event callback when it is loaded.
        After the client is started, it will assign itself to the corresponding `app` property
    """

    app: "BaseClient"
    event_dict: Dict["EventEnum", List[CLIENT_EVENT_FN]] = {}

    async def process_request(self, request: Request) -> Request:
        return request

    async def process_response(self, response: Response) -> Response:
        return response

    async def process_exc(self, response: Response) -> Response:
        return response
