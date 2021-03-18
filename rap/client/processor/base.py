from abc import ABCMeta, abstractmethod

from rap.client.model import Request, Response


class BaseProcessor(metaclass=ABCMeta):
    @abstractmethod
    async def process_request(self, request: Request) -> Request:
        raise NotImplementedError

    @abstractmethod
    async def process_response(self, response: Response) -> Response:
        raise NotImplementedError
