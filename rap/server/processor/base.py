from rap.server.model import RequestModel, ResponseModel


class BaseProcessor(object):
    """
    feat: Process the data of a certain process (usually used to read data and write data)
    ps: If you need to share data, please use `request.stats` and `response.stats`
    """
    async def process_request(self, request: RequestModel):
        pass

    async def process_response(self, response: ResponseModel):
        pass
