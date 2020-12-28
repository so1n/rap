from rap.server.model import RequestModel, ResponseModel


class BaseProcessor(object):
    async def process_request(self, request: RequestModel):
        pass

    async def process_response(self, response: ResponseModel):
        pass
