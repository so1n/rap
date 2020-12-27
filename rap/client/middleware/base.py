from rap.client.model import Request, Response


class BaseMiddleware(object):
    async def process_request(self, request: Request):
        pass

    async def process_response(self, response: Response):
        pass
