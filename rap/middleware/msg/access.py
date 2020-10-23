from typing import Callable, Coroutine, Optional

from rap.middleware.base_middleware import BaseRequestMiddleware
from rap.common.types import BASE_REQUEST_TYPE


class AccessMiddleware(BaseRequestMiddleware):
    def __init__(self):
        pass

    async def dispatch(self, request: Optional[BASE_REQUEST_TYPE], callback: Optional[Coroutine[Callable]] = None):
        pass
