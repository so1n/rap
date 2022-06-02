import asyncio
import inspect
import sys
from functools import wraps
from typing import Any, AsyncGenerator, Awaitable, Callable, Dict, List, Optional, Sequence, TypeVar

from rap.client.endpoint import BalanceEnum, BaseEndpoint, LocalEndpoint
from rap.client.model import Response
from rap.client.processor.base import BaseProcessor
from rap.client.types import CLIENT_EVENT_FN
from rap.common.cache import Cache
from rap.common.channel import UserChannelCovariantType, get_corresponding_channel_class
from rap.common.collect_statistics import WindowStatistics
from rap.common.types import T_ParamSpec as P
from rap.common.types import T_ReturnType as R_T
from rap.common.utils import EventEnum, get_func_sig, param_handle

__all__ = ["BaseClient", "Client"]
CHANNEL_F = Callable[[UserChannelCovariantType], Awaitable[None]]
Callable_T = TypeVar("Callable_T")


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

    ####################
    # wrapper func api #
    ####################
    def _wrapper_func(
        self,
        func: Callable[P, R_T],  # type: ignore
        group: Optional[str],
        header: Optional[dict] = None,
        is_private: Optional[bool] = None,
        name: str = "",
    ) -> Callable[P, Awaitable[R_T]]:  # type: ignore
        """Decorate normal function"""
        name = name if name else func.__name__
        func_sig: inspect.Signature = get_func_sig(func)

        @wraps(func)  # type: ignore
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R_T:  # type: ignore
            # Mypy Can not know ParamSpecArgs like Sequence and ParamSpecKwargs like Dict
            _args: Sequence = args  # type: ignore
            _kwargs: Dict = kwargs  # type: ignore

            _header = _kwargs.pop("header", header)
            _is_private = _kwargs.pop("is_private", is_private)
            result: Any = await self.invoke_by_name(
                name,
                arg_param=param_handle(func_sig, _args, _kwargs),
                group=group,
                header=_header,
                is_private=_is_private,
            )
            return result

        return wrapper

    def _wrapper_gen_func(
        self,
        func: Callable[P, R_T],  # type: ignore
        group: Optional[str],
        is_private: Optional[bool] = None,
        name: str = "",
    ) -> Callable[P, AsyncGenerator[R_T, None]]:  # type: ignore
        """Decoration generator function"""
        name = name if name else func.__name__
        func_sig: inspect.Signature = get_func_sig(func)

        if not inspect.isasyncgenfunction(func):
            raise TypeError(f"func:{func.__name__} must async gen function")

        @wraps(func)  # type: ignore
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> AsyncGenerator[R_T, None]:  # type: ignore
            # Mypy Can not know ParamSpecArgs like Sequence and ParamSpecKwargs like Dict
            _args: Sequence = args  # type: ignore
            _kwargs: Dict = kwargs  # type: ignore

            _is_private = _kwargs.pop("is_private", is_private)
            async with self.endpoint.picker(is_private=_is_private) as transport:
                async with transport.channel(name, group) as channel:
                    await channel.write(param_handle(func_sig, _args, _kwargs))
                    async for result in channel.get_read_channel():
                        yield result

        return wrapper

    def _wrapper_channel(
        self, func: CHANNEL_F, group: Optional[str], name: str = "", is_private: Optional[bool] = None
    ) -> Callable[..., Awaitable[None]]:
        """Decoration channel function"""
        name = name if name else func.__name__
        get_corresponding_channel_class(func)

        @wraps(func)
        async def wrapper() -> None:
            async with self.endpoint.picker(is_private=is_private) as transport:
                async with transport.channel(name, group) as channel:
                    await func(channel.get_user_channel_from_func(func))

        return wrapper

    #####################
    # register func api #
    #####################
    def register_func(
        self, name: str = "", group: Optional[str] = None
    ) -> Callable[[Callable[P, R_T]], Callable[P, Awaitable[R_T]]]:  # type: ignore
        def wrapper(func: Callable[P, R_T]) -> Callable[P, Awaitable[R_T]]:  # type: ignore
            if not inspect.iscoroutinefunction(func):
                raise TypeError(f"func:{func.__name__} must coroutine function")
            return self._wrapper_func(func, group=group, name=name)

        return wrapper

    def register_gen_func(
        self, name: str = "", group: Optional[str] = None
    ) -> Callable[[Callable[P, R_T]], Callable[P, AsyncGenerator[R_T, None]]]:  # type: ignore
        def wrapper(func: Callable[P, R_T]) -> Callable[P, AsyncGenerator[R_T, None]]:  # type: ignore
            return self._wrapper_gen_func(func, group=group, name=name)

        return wrapper

    def register_channel(
        self, name: str = "", group: Optional[str] = None, is_private: bool = False
    ) -> Callable[[CHANNEL_F], Any]:
        def wrapper(func: CHANNEL_F) -> Any:  # type: ignore
            return self._wrapper_channel(func, group=group, name=name, is_private=is_private)

        return wrapper

    def register(self, name: str = "", group: Optional[str] = None) -> Callable:
        """Using this method to decorate a fake function can help you use it better.
        (such as ide completion, ide reconstruction and type hints)
        and will be automatically registered according to the function type

        Warning: This method does not work well with Type Hints and is not recommended.

        :param name: rap func name
        :param group: func's group, default value is `default`
        """

        def wrapper(func: Callable[P, R_T]) -> Callable:  # type: ignore
            if inspect.iscoroutinefunction(func):
                return self._wrapper_func(func, group, name=name)  # type: ignore
            elif inspect.isasyncgenfunction(func):
                return self._wrapper_gen_func(func, group, name=name)  # type: ignore
            raise TypeError(f"func:{func.__name__} must coroutine function or async gen function")

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
        is_private: Optional[bool] = None,
    ) -> Response:
        """rpc client base invoke method
        Note: This method does not support parameter type checking, not support channels;
        :param name: rpc func name
        :param arg_param: rpc func param
        :param group: func's group
        :param header: request header
        :param is_private: If the value is True, it will get transport for its own use only. default False
        """
        async with self.endpoint.picker(is_private=is_private) as transport:
            return await transport.request(name, arg_param, group=group, header=header)

    async def invoke_by_name(
        self,
        name: str,
        arg_param: Optional[Sequence[Any]] = None,
        header: Optional[dict] = None,
        group: Optional[str] = None,
        is_private: Optional[bool] = None,
    ) -> Any:
        """rpc client base invoke method
        Note: This method does not support parameter type checking, not support channels;
        :param name: rpc func name
        :param arg_param: rpc func param
        :param group: func's group
        :param header: request header
        :param is_private: If the value is True, it will get transport for its own use only. default False
        """
        response: Response = await self.request(name, arg_param, group=group, header=header, is_private=is_private)
        return response.body["result"]

    def invoke(
        self,
        func: Callable[P, R_T],  # type: ignore
        header: Optional[dict] = None,
        group: Optional[str] = None,
        is_private: bool = False,
    ) -> Callable[P, Awaitable[R_T]]:  # type: ignore
        """automatically resolve function names and call invoke_by_name
        :param func: python func
        :param group: func's group, default value is `default`
        :param header: request header
        :param is_private: If the value is True, it will get transport for its own use only. default False
        """
        return self._wrapper_func(func, group=group, header=header, is_private=is_private)

    def invoke_iterator(
        self,
        func: Callable[P, R_T],  # type: ignore
        group: Optional[str] = None,
        is_private: bool = False,
    ) -> Callable[P, AsyncGenerator[R_T, None]]:  # type: ignore
        """Python-specific generator invoke
        :param func: python func
        :param group: func's group, default value is `default`
        :param is_private: If the value is True, it will get transport for its own use only. default False
        """
        return self._wrapper_gen_func(func, group=group, is_private=is_private)

    def invoke_channel(
        self,
        func: Callable[[UserChannelCovariantType], Awaitable[None]],
        group: Optional[str] = None,
        is_private: bool = False,
    ) -> Callable[..., Awaitable[None]]:
        """invoke channel fun
        :param func: python func
        :param group: func's group, default value is `default`
        :param is_private: If the value is True, it will get transport for its own use only. default False
        """
        return self._wrapper_channel(func, group=group, is_private=is_private)

    ###############################
    # Magic api (Not recommended) #
    ###############################
    def inject(self, func: Callable_T, name: str = "", group: Optional[str] = None) -> Callable_T:
        """Principle replacement function based on "sys.module
        :param func: Python func
        :param name: rap func name
        :param group: func's group, default value is `default`
        """
        if getattr(func, "raw_func", None):  # type: ignore
            raise RuntimeError(f"{func} already inject or register")
        func_module: Optional[str] = getattr(func, "__module__", None)
        if not func_module:
            raise AttributeError(f"{type(func)} object has no attribute '__module__'")

        for wrapper_func in [self._wrapper_func, self._wrapper_gen_func, self._wrapper_channel]:
            try:
                new_func: Callable_T = wrapper_func(func=func, name=name, group=group)
                break
            except TypeError:
                pass
        else:
            raise TypeError("inject func type error")

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
    def get_raw_func(func: Callable_T) -> Callable_T:
        """Get the original function"""
        raw_func: Optional[Callable_T] = getattr(func, "raw_func", None)  # type: ignore
        if not raw_func:
            raise RuntimeError("The original func could not be found, and the operation could not be executed.")
        return raw_func


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
