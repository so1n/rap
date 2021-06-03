from typing import Tuple

from rap.client.model import Request, Response


class BaseProcessor(object):
    def start_event_handle(self) -> None:
        pass

    def stop_event_handle(self) -> None:
        pass

    async def process_request(self, request: Request) -> Request:
        return request

    async def process_response(self, response: Response) -> Response:
        return response

    async def process_exc(self, response: Response, exc: Exception) -> Tuple[Response, Exception]:
        return response, exc
