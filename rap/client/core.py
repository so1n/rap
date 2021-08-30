import inspect
import sys
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, Sequence, Type

from rap.client.endpoint import BaseEndpoint, LocalEndpoint, PickConnEnum
from rap.client.model import Response
from rap.client.processor.base import BaseProcessor
from rap.client.transport.channel import Channel
from rap.client.transport.transport import Transport
from rap.client.types import CLIENT_EVENT_FN
from rap.common import event
from rap.common.cache import Cache
from rap.common.collect_statistics import WindowStatistics
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
        The server will return the invoke id of the generator function,
        and the client can continue to get data based on the invoke id.
        If no data, the server will return status_code = 301 and client must raise StopAsyncIteration Error.
        """
        response: Response = await self._client.transport.request(
            self._name,
            self._conn,
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
    def __init__(
        self,
        server_name: str,
        endpoint: BaseEndpoint,
        timeout: Optional[int] = None,
        cache_interval: Optional[float] = None,
        ws_min_interval: Optional[int] = None,
        ws_max_interval: Optional[int] = None,
        ws_statistics_interval: Optional[int] = None,
    ):
        """
        :param endpoint: rap endpoint
        :param timeout: read msg from future timeout
        :param cache_interval: Cache auto scan expire key interval
        :param ws_min_interval: WindowStatistics time per window
        :param ws_max_interval: WindowStatistics Window capacity
        :param ws_statistics_interval: WindowStatistics Statistical data interval from window
        """
        self.server_name: str = server_name
        self.endpoint: BaseEndpoint = endpoint
        self.transport: Transport = Transport(self, read_timeout=timeout)
        self._processor_list: List[BaseProcessor] = []
        self._event_dict: Dict[EventEnum, List[CLIENT_EVENT_FN]] = {
            value: [] for value in EventEnum.__members__.values()
        }
        self.cache: Cache = Cache(interval=cache_interval)
        self.window_statistics: WindowStatistics = WindowStatistics(
            interval=ws_min_interval, max_interval=ws_max_interval, statistics_interval=ws_statistics_interval
        )

        self.endpoint.set_transport(self.transport)

    ##################
    # start& close #
    ##################
    async def stop(self) -> None:
        """close client transport"""
        for handler in self._event_dict[EventEnum.before_end]:
            handler(self)  # type: ignore
        await self.endpoint.stop()
        for handler in self._event_dict[EventEnum.after_end]:
            handler(self)  # type: ignore

    async def start(self) -> None:
        """Create client transport"""
        for handler in self._event_dict[EventEnum.before_start]:
            handler(self)  # type: ignore
        await self.endpoint.start()
        for handler in self._event_dict[EventEnum.after_start]:
            handler(self)  # type: ignore

    def register_request_event_handle(self, event_class: Type[event.Event], fn: Callable[[Response], None]) -> None:
        self.transport.register_event_handle(event_class, fn)

    def unregister_request_event_handle(self, event_class: Type[event.Event], fn: Callable[[Response], None]) -> None:
        self.transport.unregister_event_handle(event_class, fn)

    def register_client_event_handle(self, event_name: EventEnum, fn: CLIENT_EVENT_FN) -> None:
        self._event_dict[event_name].append(fn)

    def load_processor(self, processor_list: List[BaseProcessor]) -> None:
        for processor in processor_list:
            processor.app = self  # type: ignore
            for event_type, handle in processor.event_dict.items():
                self._event_dict[event_type].extend(handle)
        if not self.is_close:
            for processor in processor_list:
                for handler in processor.event_dict.get(EventEnum.before_start, []):
                    handler(self)  # type: ignore
                for handler in processor.event_dict.get(EventEnum.after_start, []):
                    handler(self)  # type: ignore
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
            result: Any = await self.raw_invoke(name, param_handle(func, args, kwargs), group=group, header=header)
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
            async with self.endpoint.picker() as conn:
                async for result in AsyncIteratorCall(
                    name, self, conn, param_handle(func, args, kwargs), group=group, header=header
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
            async with self.endpoint.picker() as conn:
                async with self.transport.channel(name, conn, group) as channel:
                    return await func(channel)

        return RapFunc(wrapper, func)

    ###################
    # client base api #
    ###################
    @property
    def is_close(self) -> bool:
        """Whether the client is closed"""
        return self.endpoint.is_close

    async def raw_invoke(
        self,
        name: str,
        arg_param: Optional[Sequence[Any]] = None,
        header: Optional[dict] = None,
        group: Optional[str] = None,
    ) -> Any:
        """rpc client base invoke method
        Note: This method does not support parameter type checking, not support channels;
        name: func name
        args: func param
        header: request's header
        group: func group, default value is `default`
        """
        async with self.endpoint.picker() as conn:
            response = await self.transport.request(name, conn, arg_param, group=group, header=header)
        return response.body["result"]

    async def invoke(
        self,
        func: Callable,
        arg_param: Sequence[Any] = None,
        kwarg_param: Optional[Dict[str, Any]] = None,
        header: Optional[dict] = None,
        group: Optional[str] = None,
    ) -> Any:
        """automatically resolve function names and call raw_invoke
        func: rpc func
        args: func param
        kwargs_param: func kwargs param
        header: request's header
        group: func group, default value is `default`
        """
        if not arg_param:
            arg_param = ()
        if not kwarg_param:
            kwarg_param = {}
        return await self.raw_invoke(
            func.__name__, param_handle(func, arg_param, kwarg_param), group=group, header=header
        )

    async def iterator_invoke(
        self,
        func: Callable,
        arg_param: Sequence[Any],
        kwarg_param: Optional[Dict[str, Any]] = None,
        header: Optional[dict] = None,
        group: Optional[str] = None,
    ) -> Any:
        """Python-specific generator invoke
        func: rap func
        args: func param
        kwargs_param: func kwargs param
        header: request's header
        group: func group, default value is `default`
        """
        if not kwarg_param:
            kwarg_param = {}

        async with self.endpoint.picker() as conn:
            async for result in AsyncIteratorCall(
                func.__name__, self, conn, param_handle(func, arg_param, kwarg_param), header=header, group=group
            ):
                yield result

    def inject(self, func: Callable, name: str = "", group: Optional[str] = None) -> RapFunc:
        """Replace the function with `RapFunc` and inject it into the client at the same time"""
        if isinstance(func, RapFunc):
            raise RuntimeError(f"{func} already inject or register")
        new_func: RapFunc = self.register(name=name, group=group)(func)
        sys.modules[func.__module__].__setattr__(func.__name__, new_func)
        return new_func

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
        keep_alive_timeout: Optional[int] = None,
        ssl_crt_path: Optional[str] = None,
        cache_interval: Optional[float] = None,
        ws_min_interval: Optional[int] = None,
        ws_max_interval: Optional[int] = None,
        ws_statistics_interval: Optional[int] = None,
        select_conn_method: PickConnEnum = PickConnEnum.random,
        ping_sleep_time: Optional[int] = None,
        ping_fail_cnt: Optional[int] = None,
        wait_server_recover: bool = True,
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
        keep_alive_timeout: read msg from conn timeout
        """
        local_endpoint: LocalEndpoint = LocalEndpoint(
            conn_list,
            ssl_crt_path=ssl_crt_path,
            timeout=keep_alive_timeout,
            pack_param=None,
            unpack_param=None,
            pick_conn_method=select_conn_method,
            ping_fail_cnt=ping_fail_cnt,
            ping_sleep_time=ping_sleep_time,
            wait_server_recover=wait_server_recover,
        )
        super().__init__(
            server_name,
            local_endpoint,
            timeout,
            cache_interval=cache_interval,
            ws_min_interval=ws_min_interval,
            ws_max_interval=ws_max_interval,
            ws_statistics_interval=ws_statistics_interval,
        )
