from typing import Any, Callable, Union


class BaseMiddleware(object):
    async def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return await self.dispatch(*args)

    def load_sub_middleware(self, call_next: "Union[Callable, BaseMiddleware]") -> None:
        if isinstance(call_next, BaseMiddleware):
            setattr(self, self.call_next.__name__, call_next.dispatch)
        else:
            setattr(self, self.call_next.__name__, call_next)

    async def call_next(self, *args: Any) -> Any:
        raise NotImplementedError

    async def dispatch(self, *args: Any) -> Any:
        raise NotImplementedError
