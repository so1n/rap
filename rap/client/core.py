import asyncio
import inspect
import sys
from functools import wraps
from typing import Any, Awaitable, Callable, Dict, List, Optional, Sequence, Type, TypeVar

from rap.client.endpoint import BalanceEnum, BaseEndpoint, LocalEndpoint
from rap.client.model import Response
from rap.client.processor.base import BaseProcessor
from rap.client.transport.async_iterator import AsyncIteratorCall
from rap.client.types import CLIENT_EVENT_FN
from rap.common.cache import Cache
from rap.common.channel import UserChannel
from rap.common.collect_statistics import WindowStatistics
from rap.common.types import T_ParamSpec, T_ReturnType, is_type
from rap.common.utils import EventEnum, RapFunc, param_handle

__all__ = ["BaseClient", "Client"]
CHANNEL_F = Callable[[UserChannel], Awaitable[None]]
Callable_T = TypeVar("Callable_T")
# Python version info < 3.9 not support Callable[P, T]
# CHANNEL_F = Callable[Concatenate[UserChannel], Any]  # type: ignore


class BaseClient:
    """Human-friendly `rap client` api"""

    endpoint: BaseEndpoint

    def __init__(
        self,
        server_name: str,
        cache_interval: Optional[float] = None,
        ws_min_interval: Optional[int] = None,
        ws_max_interval: Optional[int] = None,
        ws_statistics_interval: Optional[int] = None,
        through_deadline: bool = False,
    ):
        """
        :param server_name: server name
        :param cache_interval: Cache auto scan expire key interval
        :param ws_min_interval: WindowStatistics time per window
        :param ws_max_interval: WindowStatistics Window capacity
        :param ws_statistics_interval: WindowStatistics Statistical data interval from window
        :param through_deadline: enable through deadline to server
        """
        self.server_name: str = server_name
        self._processor_list: List[BaseProcessor] = []
        self._through_deadline: bool = through_deadline
        self._event_dict: Dict[EventEnum, List[CLIENT_EVENT_FN]] = {
            value: [] for value in EventEnum.__members__.values()
        }
        self._cache: Cache = Cache(interval=cache_interval)
        self._window_statistics: WindowStatistics = WindowStatistics(
            interval=ws_min_interval, max_interval=ws_max_interval, statistics_interval=ws_statistics_interval
        )

    @property
    def cache(self) -> Cache:
        return self._cache

    @property
    def window_statistics(self) -> WindowStatistics:
        return self._window_statistics

    @property
    def through_deadline(self) -> bool:
        return self._through_deadline

    ##################
    # start & close #
    ################
    async def stop(self) -> None:
        """close client transport"""
        for handler in self._event_dict[EventEnum.before_end]:
            ret: Any = handler(self)  # type: ignore
            if asyncio.iscoroutine(ret):
                await ret  # type: ignore
        await self.endpoint.stop()
        for handler in self._event_dict[EventEnum.after_end]:
            ret: Any = handler(self)  # type: ignore
            if asyncio.iscoroutine(ret):
                await ret  # type: ignore

    async def start(self) -> None:
        """Create client transport"""
        for handler in self._event_dict[EventEnum.before_start]:
            ret: Any = handler(self)  # type: ignore
            if asyncio.iscoroutine(ret):
                await ret  # type: ignore
        await self.endpoint.start()
        for handler in self._event_dict[EventEnum.after_start]:
            ret: Any = handler(self)  # type: ignore
            if asyncio.iscoroutine(ret):
                await ret  # type: ignore

    def _check_run(self) -> None:
        if not self.is_close:
            raise RuntimeError("The method cannot be called on the run")

    ########################
    # client event handler #
    ########################
    def register_client_event_handle(self, event_name: EventEnum, fn: CLIENT_EVENT_FN) -> None:
        self._check_run()
        self._event_dict[event_name].append(fn)

    ############################
    # client processor handler #
    ############################
    def load_processor(self, processor_list: List[BaseProcessor]) -> None:
        self._check_run()
        for processor in processor_list:
            processor.app = self  # type: ignore
            for event_type, handle in processor.event_dict.items():
                self._event_dict[event_type].extend(handle)
        self._processor_list.extend(processor_list)

    @property
    def processor_list(self) -> List[BaseProcessor]:
        return self._processor_list

    #####################
    # register func api #
    #####################
    def _async_register(
        self, func: Callable[T_ParamSpec, T_ReturnType], group: Optional[str], name: str = ""  # type: ignore
    ) -> Callable[T_ParamSpec, Awaitable[T_ReturnType]]:  # type: ignore
        """Decorate normal function"""
        name = name if name else func.__name__
        func_sig: inspect.Signature = inspect.signature(func)
        return_type: Type = func_sig.return_annotation

        @wraps(func)  # type: ignore
        async def wrapper(*args: T_ParamSpec.args, **kwargs: T_ParamSpec.kwargs) -> T_ReturnType:  # type: ignore
            # Mypy Can not know ParamSpecArgs like Sequence and ParamSpecKwargs like Dict
            _args: Sequence = args  # type: ignore
            _kwargs: Dict = kwargs  # type: ignore

            header: Optional[dict] = _kwargs.pop("header", None)
            result: Any = await self.raw_invoke(
                name, param_handle(func_sig, _args, _kwargs), group=group, header=header
            )
            if not is_type(return_type, type(result)):
                raise RuntimeError(f"{func} return type is {return_type}, but result type is {type(result)}")
            return result

        return wrapper

    def _async_gen_register(
        self, func: Callable[T_ParamSpec, T_ReturnType], group: Optional[str], name: str = ""  # type: ignore
    ) -> Callable[T_ParamSpec, T_ReturnType]:  # type: ignore
        """Decoration generator function"""
        name = name if name else func.__name__
        func_sig: inspect.Signature = inspect.signature(func)
        return_type: Type = func_sig.return_annotation

        @wraps(func)  # type: ignore
        async def wrapper(*args: T_ParamSpec.args, **kwargs: T_ParamSpec.kwargs) -> T_ReturnType:
            # Mypy Can not know ParamSpecArgs like Sequence and ParamSpecKwargs like Dict
            _args: Sequence = args  # type: ignore
            _kwargs: Dict = kwargs  # type: ignore

            header: Optional[dict] = _kwargs.pop("header", None)
            is_private: bool = _kwargs.pop("is_private", False)
            async with self.endpoint.picker(is_private=is_private) as transport:
                async for result in AsyncIteratorCall(
                    name,
                    transport,
                    param_handle(func_sig, _args, _kwargs),
                    group=group,
                    header=header,
                ):
                    if not is_type(return_type, type(result)):
                        raise RuntimeError(f"{func} return type is {return_type}, but result type is {type(result)}")
                    yield result

        return wrapper

    def _async_channel_register(
        self, func: CHANNEL_F, group: Optional[str], name: str = "", is_private: bool = False
    ) -> Callable:
        """Decoration channel function"""
        name = name if name else func.__name__

        @wraps(func)
        async def wrapper() -> None:
            async with self.endpoint.picker(is_private=is_private) as transport:
                async with transport.channel(name, group) as channel:
                    await func(channel)

        return wrapper

    ###################
    # client base api #
    ###################
    @property
    def is_close(self) -> bool:
        """Whether the client is closed"""
        return self.endpoint.is_close

    async def request(
        self,
        name: str,
        arg_param: Optional[Sequence[Any]] = None,
        header: Optional[dict] = None,
        group: Optional[str] = None,
        is_private: bool = False,
    ) -> Response:
        """rpc client base invoke method
        Note: This method does not support parameter type checking, not support channels;
        :param name: rpc func name
        :param arg_param: rpc func param
        :param group: func's group
        :param header: request header
        :param is_private: If the value is True, it will get transport for its own use only
        """
        async with self.endpoint.picker(is_private=is_private) as transport:
            return await transport.request(name, arg_param, group=group, header=header)

    async def raw_invoke(
        self,
        name: str,
        arg_param: Optional[Sequence[Any]] = None,
        header: Optional[dict] = None,
        group: Optional[str] = None,
        is_private: bool = False,
    ) -> Any:
        """rpc client base invoke method
        Note: This method does not support parameter type checking, not support channels;
        :param name: rpc func name
        :param arg_param: rpc func param
        :param group: func's group
        :param header: request header
        :param is_private: If the value is True, it will get transport for its own use only
        """
        response: Response = await self.request(name, arg_param, group=group, header=header, is_private=is_private)
        return response.body["result"]

    def invoke(
        self,
        func: Callable[T_ParamSpec, T_ReturnType],  # type: ignore
        header: Optional[dict] = None,
        group: Optional[str] = None,
        is_private: bool = False,
    ) -> Callable[T_ParamSpec, Awaitable[T_ReturnType]]:  # type: ignore
        """automatically resolve function names and call raw_invoke
        :param func: python func
        :param group: func's group, default value is `default`
        :param header: request header
        :param is_private: If the value is True, it will get transport for its own use only
        """

        async def wrapper(*args: T_ParamSpec.args, **kwargs: T_ParamSpec.kwargs) -> T_ReturnType:
            # Mypy Can not know ParamSpecArgs like Sequence and ParamSpecKwargs like Dict
            _args: Sequence = args or ()  # type: ignore
            _kwargs: Dict = kwargs or {}  # type: ignore

            arg_param = param_handle(inspect.signature(func), _args, _kwargs)
            return await self.raw_invoke(func.__name__, arg_param, group=group, header=header, is_private=is_private)

        return wrapper

    async def iterator_invoke(
        self,
        func: Callable,
        arg_param: Sequence[Any],
        kwarg_param: Optional[Dict[str, Any]] = None,
        header: Optional[dict] = None,
        group: Optional[str] = None,
        is_private: bool = False,
    ) -> Any:
        """Python-specific generator invoke
        :param func: python func
        :param arg_param: rpc func param
        :param kwarg_param: func kwargs param
        :param group: func's group, default value is `default`
        :param header: request header
        :param is_private: If the value is True, it will get transport for its own use only
        """
        if not inspect.isasyncgenfunction(func):
            raise TypeError("func must be async gen function")
        async with self.endpoint.picker(is_private=is_private) as transport:
            async for result in AsyncIteratorCall(
                func.__name__,
                transport,
                param_handle(inspect.signature(func), arg_param or (), kwarg_param or {}),
                header=header,
                group=group,
            ):
                yield result

    def inject(self, func: Callable_T, name: str = "", group: Optional[str] = None) -> Callable_T:
        """Replace the function with `RapFunc` and inject it into the client at the same time
        :param func: Python func
        :param name: rap func name
        :param group: func's group, default value is `default`
        """
        if getattr(func, "raw_func", None):  # type: ignore
            raise RuntimeError(f"{func} already inject or register")
        func_module: Optional[str] = getattr(func, "__module__", None)
        if not func_module:
            raise AttributeError(f"{type(func)} object has no attribute '__module__'")
        new_func: Callable_T = self.register(name=name, group=group)(func)
        setattr(new_func, "raw_func", func)
        sys.modules[func_module].__setattr__(func.__name__, new_func)  # type: ignore
        return new_func

    @staticmethod
    def recovery(func: Callable_T) -> Callable_T:
        """Restore functions that have been injected"""
        raw_func: Optional[Callable_T] = getattr(func, "raw_func", None)  # type: ignore
        if not raw_func:
            raise RuntimeError("The original func could not be found, and the operation could not be executed.")
        func_module: Optional[str] = getattr(raw_func, "__module__", None)
        if not func_module:
            raise AttributeError(f"{type(func)} object has no attribute '__module__'")
        sys.modules[func_module].__setattr__(func.__name__, raw_func)  # type: ignore
        return raw_func

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

        :param name: rap func name
        :param group: func's group, default value is `default`
        """

        def wrapper(func: Callable[T_ParamSpec, T_ReturnType]) -> Callable:  # type: ignore
            if not (inspect.iscoroutinefunction(func) or inspect.isasyncgenfunction(func)):
                raise TypeError(f"func:{func.__name__} must coroutine function or async gen function")
            func_sig: inspect.Signature = inspect.signature(func)
            func_arg_parameter: List[inspect.Parameter] = [
                i for i in func_sig.parameters.values() if i.default == i.empty
            ]
            if len(func_arg_parameter) == 1 and func_arg_parameter[0].annotation is UserChannel:
                return self._async_channel_register(func, group, name=name)  # type: ignore
            if inspect.iscoroutinefunction(func):
                return self._async_register(func, group, name=name)  # type: ignore
            elif inspect.isasyncgenfunction(func):
                return self._async_gen_register(func, group, name=name)  # type: ignore
            raise TypeError(f"func:{func.__name__} must coroutine function or async gen function")

        return wrapper


class Client(BaseClient):
    def __init__(
        self,
        server_name: str,
        conn_list: List[dict],
        keep_alive_timeout: Optional[int] = None,
        ssl_crt_path: Optional[str] = None,
        cache_interval: Optional[float] = None,
        ws_min_interval: Optional[int] = None,
        ws_max_interval: Optional[int] = None,
        ws_statistics_interval: Optional[int] = None,
        through_deadline: bool = False,
        select_conn_method: BalanceEnum = BalanceEnum.random,
        min_ping_interval: Optional[int] = None,
        max_ping_interval: Optional[int] = None,
        ping_fail_cnt: Optional[int] = None,
        max_pool_size: Optional[int] = None,
        min_poll_size: Optional[int] = None,
    ):
        """
        server_name: server name
        transport_list: client transport info
          include ip, port, weight
          ip: server ip
          port: server port
          weight: select this transport weight
          e.g.  [{"ip": "localhost", "port": "9000", weight: 10}]
        keep_alive_timeout: read msg from transport timeout
        """

        super().__init__(
            server_name,
            cache_interval=cache_interval,
            ws_min_interval=ws_min_interval,
            ws_max_interval=ws_max_interval,
            ws_statistics_interval=ws_statistics_interval,
            through_deadline=through_deadline,
        )
        self.endpoint = LocalEndpoint(
            conn_list,
            self,  # type: ignore
            ssl_crt_path=ssl_crt_path,
            pack_param=None,
            unpack_param=None,
            read_timeout=keep_alive_timeout,
            balance_enum=select_conn_method,
            ping_fail_cnt=ping_fail_cnt,
            min_ping_interval=min_ping_interval,
            max_ping_interval=max_ping_interval,
            max_pool_size=max_pool_size,
            min_poll_size=min_poll_size,
        )
