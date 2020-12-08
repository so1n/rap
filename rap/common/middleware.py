from typing import Any, Callable, Union


class BaseMiddleware(object):
    async def __call__(self, *args, **kwargs):
        return await self.dispatch(*args)

    async def dispatch(self, *args: Any):
        raise NotImplementedError

    def load_sub_middleware(self, call_next: "Union[Callable, BaseMiddleware]"):
        if isinstance(call_next, BaseMiddleware):
            self.call_next = call_next.call_next
        else:
            self.call_next = call_next

    async def call_next(self, value: Any):
        pass
