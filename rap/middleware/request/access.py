from typing import Callable, Coroutine, Optional

from rap.middleware.base_middleware import BaseMiddleware
from rap.common.types import BASE_REQUEST_TYPE


class AccessMiddleware(BaseMiddleware):
    def __init__(self):
        pass

    async def dispatch(self, request: Optional[BASE_REQUEST_TYPE]):
        pass
