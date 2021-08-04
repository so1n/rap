import inspect
import sys
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, Sequence, Type

from rap.client.endpoint import BaseEndpoint, LocalEndpoint, SelectConnEnum
from rap.client.model import Response
from rap.client.processor.base import BaseProcessor
from rap.client.transport.channel import Channel
from rap.client.transport.transport import Transport
from rap.common import event
from rap.common.conn import Connection
from rap.common.types import is_type
from rap.common.utils import EventEnum, RapFunc, param_handle

__all__ = ["BaseClient", "Client"]
CHANNEL_F = Callable[[Channel], Any]


class AsyncIteratorCall:
    """let client support async iterator (keep sending and receiving messages under the same conn)"""

    def __init__(
        self,
        name: str,
        client: "BaseClient",
        conn: Connection,
        arg_param: Sequence[Any],
        header: Optional[dict] = None,
        group: Optional[str] = None,
    ):
        """
        name: func name
        client: rap base client
        conn: rap tcp conn
        arg_param: func param
        header: request's header
        group: func group, default value is `default`
        """
        self._name: str = name
        self._client: "BaseClient" = client
        self._conn: Connection = conn
        self._call_id: Optional[int] = None
        self._arg_param: Sequence[Any] = arg_param
        self._header: Optional[dict] = header or {}
        self.group: Optional[str] = group

    #####################
    # async for support #
    #####################
    def __aiter__(self) -> "AsyncIteratorCall":
        return self

    async def __anext__(self) -> Any:
        """
        The server will return the call id of the generator function,
        and the client can continue to get data based on the call id.
        If no data, the server will return status_code = 301 and client must raise StopAsyncIteration Error.
        """
        response: Response = await self._client.transport.request(
            self._name,
            [self._conn],
            arg_param=self._arg_param,
            call_id=self._call_id,
            header=self._header,
            group=self.group,
        )
        if response.status_code == 301:
            raise StopAsyncIteration()
        self._call_id = response.body["call_id"]
        return response.body["result"]


class BaseClient:
    def __init__(self, endpoint: BaseEndpoint, timeout: Optional[int] = None):
        """
        endpoint: rap endpoint
        read_timeout: read msg from future timeout
        """
        self.endpoint: BaseEndpoint = endpoint
        self.transport: Transport = Transport(endpoint.server_name, read_timeout=timeout)
        self._processor_list: List[BaseProcessor] = []
        self._event_dict: Dict[EventEnum, List[Callable]] = {value: [] for value in EventEnum.__members__.values()}

        self.endpoint.set_transport(self.transport)

    ##################
    # start& close #
    ##################
    async def stop(self) -> None:
        """close client transport"""
        for handler in self._event_dict[EventEnum.before_end]:
            handler()
        await self.endpoint.stop()
        for handler in self._event_dict[EventEnum.after_end]:
            handler()

    async def start(self) -> None:
        """Create client transport"""
        for handler in self._event_dict[EventEnum.before_start]:
            handler()
        await self.endpoint.start()
        for handler in self._event_dict[EventEnum.after_start]:
            handler()

    def register_event_handle(self, event_class: Type[event.Event], fn: Callable[[Response], None]) -> None:
        self.transport.register_event_handle(event_class, fn)

    def unregister_event_handle(self, event_class: Type[event.Event], fn: Callable[[Response], None]) -> None:
        self.transport.unregister_event_handle(event_class, fn)

    def load_processor(self, processor_list: List[BaseProcessor]) -> None:
        if not self.is_close:
            for processor in processor_list:
                for handler in processor.event_dict.get(EventEnum.before_start, []):
                    handler()
                for handler in processor.event_dict.get(EventEnum.after_start, []):
                    handler()
        for processor in processor_list:
            for event_type, handle in processor.event_dict.items():
                self._event_dict[event_type].extend(handle)
        self.transport.load_processor(processor_list)

    #####################
    # register func api #
    #####################
    def _async_register(self, func: Callable, group: Optional[str], name: str = "") -> RapFunc:
        """Decorate normal function"""
        name = name if name else func.__name__
        return_type: Type = inspect.signature(func).return_annotation

        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            header: Optional[dict] = kwargs.pop("header", None)
            result: Any = await self.raw_call(name, param_handle(func, args, kwargs), group=group, header=header)
            if not is_type(return_type, type(result)):
                raise RuntimeError(f"{func} return type is {return_type}, but result type is {type(result)}")
            return result

        return RapFunc(wrapper, func)

    def _async_gen_register(self, func: Callable, group: Optional[str], name: str = "") -> RapFunc:
        """Decoration generator function"""
        name = name if name else func.__name__
        return_type: Type = inspect.signature(func).return_annotation

        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            header: Optional[dict] = kwargs.pop("header", None)
            async for result in AsyncIteratorCall(
                name, self, self.get_conn(), param_handle(func, args, kwargs), group=group, header=header
            ):
                if not is_type(return_type, type(result)):
                    raise RuntimeError(f"{func} return type is {return_type}, but result type is {type(result)}")
                yield result

        return RapFunc(wrapper, func)

    def _async_channel_register(self, func: CHANNEL_F, group: Optional[str], name: str = "") -> RapFunc:
        """Decoration channel function"""
        name = name if name else func.__name__

        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            async with self.transport.channel(name, self.get_conn(), group) as channel:
                return await func(channel)

        return RapFunc(wrapper, func)

    ###################
    # client base api #
    ###################
    def get_conn(self) -> Connection:
        """get random conn from endpoint"""
        return self.endpoint.get_conn()

    def get_conn_list(self, cnt: Optional[int] = None) -> List[Connection]:
        """get conn list from endpoint. default cnt value: half of the currently available conn"""
        return self.endpoint.get_conn_list(cnt)

    @property
    def is_close(self) -> bool:
        """Whether the client is closed"""
        return self.endpoint.is_close

    async def raw_call(
        self,
        name: str,
        arg_param: Optional[Sequence[Any]] = None,
        header: Optional[dict] = None,
        group: Optional[str] = None,
    ) -> Any:
        """rpc client base call method
        Note: This method does not support parameter type checking, nor does it support channels;
        name: func name
        args: func param
        header: request's header
        group: func group, default value is `default`
        """
        conn_list: List[Connection] = self.endpoint.get_conn_list()
        response = await self.transport.request(name, conn_list, arg_param, group=group, header=header)
        return response.body["result"]

    async def call(
        self,
        func: Callable,
        arg_param: Sequence[Any],
        kwarg_param: Optional[Dict[str, Any]] = None,
        header: Optional[dict] = None,
        group: Optional[str] = None,
    ) -> Any:
        """automatically resolve function names and call raw_call
        func: rpc func
        args: func param
        kwargs_param: func kwargs param
        header: request's header
        group: func group, default value is `default`
        """
        if not kwarg_param:
            kwarg_param = {}
        return await self.raw_call(
            func.__name__, param_handle(func, arg_param, kwarg_param), group=group, header=header
        )

    async def iterator_call(
        self,
        func: Callable,
        arg_param: Sequence[Any],
        kwarg_param: Optional[Dict[str, Any]] = None,
        header: Optional[dict] = None,
        group: Optional[str] = None,
    ) -> Any:
        """Python-specific generator call
        func: rap func
        args: func param
        kwargs_param: func kwargs param
        header: request's header
        group: func group, default value is `default`
        """
        if not kwarg_param:
            kwarg_param = {}
        async for result in AsyncIteratorCall(
            func.__name__, self, self.get_conn(), param_handle(func, arg_param, kwarg_param), header=header, group=group
        ):
            yield result

    def inject(self, func: Callable, name: str = "", group: Optional[str] = None) -> None:
        """Replace the function with `RapFunc` and inject it into the client at the same time"""
        if isinstance(func, RapFunc):
            raise RuntimeError(f"{func} already inject or register")
        new_func: Callable = self.register(name=name, group=group)(func)
        sys.modules[func.__module__].__setattr__(func.__name__, new_func)

    @staticmethod
    def recovery(func: RapFunc) -> None:
        """Restore functions that have been injected"""
        if not isinstance(func, RapFunc):
            raise RuntimeError(f"{func} is not {RapFunc}, which can not recovery")
        sys.modules[func.__module__].__setattr__(func.__name__, func.raw_func)

    @staticmethod
    def get_raw_func(func: RapFunc) -> Callable:
        """Get the original function encapsulated by `RapFunc`"""
        if not isinstance(func, RapFunc):
            raise TypeError(f"{func} is not {RapFunc}")
        return func.raw_func

    def register(self, name: str = "", group: Optional[str] = None) -> Callable:
        """Using this method to decorate a fake function can help you use it better.
        (such as ide completion, ide reconstruction and type hints)
        and will be automatically registered according to the function type

        name: func name
        group: func group, default value is `default`
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
                return self._async_register(func, group, name=name)
            elif inspect.isasyncgenfunction(func):
                return self._async_gen_register(func, group, name=name)
            raise TypeError(f"func:{func.__name__} must coroutine function or async gen function")

        return wrapper


class Client(BaseClient):
    def __init__(
        self,
        server_name: str,
        conn_list: List[dict],
        timeout: Optional[int] = None,
        keep_alive_time: Optional[int] = None,
        ssl_crt_path: Optional[str] = None,
        select_conn_method: SelectConnEnum = SelectConnEnum.random,
    ):
        """
        server_name: server name
        conn_list: client conn info
          include ip, port, weight
          ip: server ip
          port: server port
          weight: select this conn weight
          e.g.  [{"ip": "localhost", "port": "9000", weight: 10}]
        timeout: read response from consumer timeout
        keep_alive_time: read msg from conn timeout
        """
        super().__init__(
            LocalEndpoint(
                server_name,
                conn_list,
                ssl_crt_path=ssl_crt_path,
                timeout=keep_alive_time,
                select_conn_method=select_conn_method,
            ),
            timeout,
        )
