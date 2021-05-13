import importlib
import inspect
import sys
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Type

from rap.client.model import Response
from rap.client.processor.base import BaseProcessor
from rap.client.transport.channel import Channel
from rap.client.transport.transport import Session, Transport
from rap.common.types import is_type
from rap.common.utils import RapFunc, check_func_type

__all__ = ["Client"]
CHANNEL_F = Callable[[Channel], Any]


class AsyncIteratorCall:
    """let client support async iterator (keep sending and receiving messages under the same conn)"""

    def __init__(
        self,
        name: str,
        client: "Client",
        arg_param: Sequence[Any],
        kwarg_param: Optional[Dict[str, Any]] = None,
        header: Optional[dict] = None,
        group: Optional[str] = None,
        session: Optional[Session] = None,
    ):
        self.group: Optional[str] = group
        self._name: str = name
        self._call_id: Optional[int] = None
        self._arg_param: Sequence[Any] = arg_param
        self._kwarg_param: Optional[Dict[str, Any]] = kwarg_param
        self._client: "Client" = client
        self._header: Optional[dict] = header

        if not session:
            session = self._client.transport.get_now_session()
            if not session:
                session = self._client.transport.session

        self._session: Session = session
        self.in_session: bool = self._session.in_session

    ###################
    # session support #
    ###################
    async def __aenter__(self) -> "AsyncIteratorCall":
        if not self.in_session:
            self._session.create()
        return self

    async def __aexit__(self, *args: Tuple) -> None:
        if not self.in_session:
            self._session.close()

    #####################
    # async for support #
    #####################
    def __aiter__(self) -> "AsyncIteratorCall":
        return self

    async def __anext__(self) -> Any:
        """
        The server will return the call id of the generator function,
        and the client can continue to get data based on the call id.
        If no data, the server will return header.status_code = 301 and client must raise StopAsyncIteration Error.
        """
        response: Response = await self._client.transport.request(
            self._name,
            self._arg_param,
            kwarg_param=self._kwarg_param,
            call_id=self._call_id,
            header=self._header,
            session=self._session,
            group=self.group,
        )
        if response.header["status_code"] == 301:
            raise StopAsyncIteration()
        self._call_id = response.body["call_id"]
        return response.body["result"]


class Client:
    def __init__(
        self,
        host_list: Optional[List[str]] = None,
        timeout: int = 9,
        keep_alive_time: int = 1200,
        ssl_crt_path: Optional[str] = None,
    ):
        """
        host_list:
         server host
         example value: ['127.0.0.1:9000', '127.0.0.1:9001']
        timeout:
         send msg timeout
        keep_alive_time
         recv msg timeout
        ssl_crt_path:
         ssl.crt  path
         example value: "./rap_ssl.crt"
        """
        if not host_list:
            host_list = ["localhost:9000"]
        self.transport: Transport = Transport(
            host_list, timeout=timeout, keep_alive_time=keep_alive_time, ssl_crt_path=ssl_crt_path
        )

    ##################
    # connect& close #
    ##################
    async def await_close(self) -> None:
        """close client transport"""
        await self.transport.await_close()

    async def connect(self) -> None:
        """Create client transport"""
        await self.transport.connect()

    def load_processor(self, processor_list: List[BaseProcessor]) -> None:
        self.transport.load_processor(processor_list)

    @property
    def session(self) -> "Session":
        return self.transport.session

    #####################
    # register func api #
    #####################
    def _async_register(
        self, func: Callable, group: Optional[str], name: str = "", enable_type_check: bool = True
    ) -> RapFunc:
        """Decorate normal function"""
        name = name if name else func.__name__
        return_type: Type = inspect.signature(func).return_annotation

        @wraps(func)
        async def type_check_wrapper(*args: Any, **kwargs: Any) -> Any:
            session: Optional[Session] = kwargs.pop("session") if "session" in kwargs else None
            header: Optional[dict] = kwargs.pop("header") if "header" in kwargs else None
            check_func_type(func, args, kwargs)
            result: Any = await self.raw_call(
                name, args, kwarg_param=kwargs, group=group, session=session, header=header
            )
            if not is_type(return_type, type(result)):
                raise RuntimeError(f"{func} return type is {return_type}, but result type is {type(result)}")
            return result

        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            session: Optional[Session] = kwargs.pop("session") if "session" in kwargs else None
            header: Optional[dict] = kwargs.pop("header") if "header" in kwargs else None
            return await self.raw_call(name, args, kwarg_param=kwargs, group=group, session=session, header=header)

        new_func: Callable = type_check_wrapper if enable_type_check else wrapper
        return RapFunc(new_func, func)

    def _async_gen_register(
        self, func: Callable, group: Optional[str], name: str = "", enable_type_check: bool = True
    ) -> RapFunc:
        """Decoration generator function"""
        name = name if name else func.__name__
        return_type: Type = inspect.signature(func).return_annotation

        @wraps(func)
        async def type_check_wrapper(*args: Any, **kwargs: Any) -> Any:
            session: Optional[Session] = kwargs.pop("session") if "session" in kwargs else None
            header: Optional[dict] = kwargs.pop("header") if "header" in kwargs else None
            check_func_type(func, args, kwargs)
            async with AsyncIteratorCall(
                name, self, args, kwarg_param=kwargs, group=group, session=session, header=header
            ) as async_iterator:
                async for result in async_iterator:
                    if not is_type(return_type, type(result)):
                        raise RuntimeError(f"{func} return type is {return_type}, but result type is {type(result)}")
                    yield result

        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            session: Optional[Session] = kwargs.pop("session") if "session" in kwargs else None
            header: Optional[dict] = kwargs.pop("header") if "header" in kwargs else None
            async with AsyncIteratorCall(
                name, self, args, kwarg_param=kwargs, group=group, session=session, header=header
            ) as async_iterator:
                async for result in async_iterator:
                    yield result

        new_func: Callable = type_check_wrapper if enable_type_check else wrapper
        return RapFunc(new_func, func)

    def _async_channel_register(self, func: CHANNEL_F, group: Optional[str], name: str = "") -> RapFunc:
        """Decoration channel function"""
        name = name if name else func.__name__

        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            async with self.transport.channel(name, group) as channel:
                return await func(channel)

        return RapFunc(wrapper, func)

    ###################
    # client base api #
    ###################
    async def raw_call(
        self,
        name: str,
        arg_param: Optional[Sequence[Any]] = None,
        kwarg_param: Optional[Dict[str, Any]] = None,
        header: Optional[dict] = None,
        group: Optional[str] = None,
        session: Optional["Session"] = None,
    ) -> Any:
        """rpc client base call method
        Note: This method does not support parameter type checking, nor does it support channels;
        name: func name
        args: python args
        header: request's header
        group: func group, default group value is `default`
        session: conn session
        """
        response = await self.transport.request(
            name, arg_param, kwarg_param, group=group, header=header, session=session
        )
        return response.body["result"]

    async def call(
        self,
        func: Callable,
        arg_param: Sequence[Any],
        kwarg_param: Optional[Dict[str, Any]] = None,
        header: Optional[dict] = None,
        group: Optional[str] = None,
        session: Optional["Session"] = None,
    ) -> Any:
        """automatically resolve function names and call raw_call
        func: rpc func
        args: python args
        header: request's header
        group: func's group, default group value is `default`
        session: conn session
        """
        return await self.raw_call(
            func.__name__, arg_param, kwarg_param=kwarg_param, group=group, header=header, session=session
        )

    async def iterator_call(
        self,
        func: Callable,
        arg_param: Sequence[Any],
        kwarg_param: Optional[Dict[str, Any]] = None,
        header: Optional[dict] = None,
        group: Optional[str] = None,
        session: Optional["Session"] = None,
    ) -> Any:
        """Python-specific generator call
        func: rap func
        args: python args
        header: request's header
        group: func's group, default group value is `default`
        session: conn session
        """
        async with AsyncIteratorCall(
            func.__name__, self, arg_param, kwarg_param=kwarg_param, header=header, group=group, session=session
        ) as async_iterator:
            async for result in async_iterator:
                yield result

    def inject(
        self, func: Callable, name: str = "", group: Optional[str] = None, enable_type_check: bool = True
    ) -> None:
        if isinstance(func, RapFunc):
            raise RuntimeError(f"{func} already inject or register")
        new_func: Callable = self.register(name=name, group=group, enable_type_check=enable_type_check)(func)
        sys.modules[func.__module__].__setattr__(func.__name__, new_func)

    @staticmethod
    def recovery(func: RapFunc) -> Any:
        if not isinstance(func, RapFunc):
            raise RuntimeError(f"{func} is not {RapFunc}, which can not recovery")
        sys.modules[func.__module__].__setattr__(func.__name__, func.raw_func)

    @staticmethod
    def get_raw_func(func: RapFunc) -> Callable:
        if not isinstance(func, RapFunc):
            raise TypeError(f"{func} is not {RapFunc}")
        return func.raw_func

    def register(
        self, name: str = "", group: Optional[str] = None, enable_type_check: bool = True
    ) -> Callable[[Callable], Callable]:
        """Using this method to decorate a fake function can help you use it better.
        (such as ide completion, ide reconstruction and type hints)
        and will be automatically registered according to the function type

        group: func group, default group value is `default`
        """

        def wrapper(func: Callable) -> RapFunc:
            if not (inspect.iscoroutinefunction(func) or inspect.isasyncgenfunction(func)):
                raise TypeError(f"func:{func.__name__} must coroutine function or async gen function")
            func_sig: inspect.Signature = inspect.signature(func)
            func_arg_parameter: List[inspect.Parameter] = [
                i for i in func_sig.parameters.values() if i.default == i.empty
            ]
            if len(func_arg_parameter) == 1 and func_arg_parameter[0].annotation is Channel:
                return self._async_channel_register(func, group, name=name)
            if inspect.iscoroutinefunction(func):
                return self._async_register(func, group, name=name, enable_type_check=enable_type_check)
            elif inspect.isasyncgenfunction(func):
                return self._async_gen_register(func, group, name=name, enable_type_check=enable_type_check)
            raise TypeError(f"func:{func.__name__} must coroutine function or async gen function")

        return wrapper
