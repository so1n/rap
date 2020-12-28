from rap.client.model import Request, Response


class BaseProcessor(object):
    async def process_request(self, request: Request):
        pass

    async def process_response(self, response: Response):
        pass
