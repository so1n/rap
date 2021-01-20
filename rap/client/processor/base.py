from rap.client.model import Request, Response


class BaseProcessor(object):
    async def process_request(self, request: Request) -> Request:
        return request

    async def process_response(self, response: Response) -> Response:
        return response
