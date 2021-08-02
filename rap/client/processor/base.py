from typing import Callable, Dict, List, Tuple

from rap.client.model import Request, Response
from rap.common.utils import EventEnum


class BaseProcessor(object):

    event_dict: Dict["EventEnum", List[Callable]] = {}

    async def process_request(self, request: Request) -> Request:
        return request

    async def process_response(self, response: Response) -> Response:
        return response

    async def process_exc(self, response: Response, exc: Exception) -> Tuple[Response, Exception]:
        return response, exc
