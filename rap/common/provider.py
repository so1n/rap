from typing import Any, Generic, Type, TypeVar

_T = TypeVar("_T")


class Provider(Generic[_T]):
    def __init__(self, element: Type[_T], *args: Any, **kwargs: Any):
        self._element: Type[_T] = element
        self._args: Any = args
        self._kwargs: Any = kwargs

    def create_instance(self, *args: Any, **kwargs: Any) -> _T:
        args = args or self._args
        kwargs.update(self._kwargs)
        return self._element(*args, **kwargs)
