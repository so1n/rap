import asyncio
import inspect
import sys
from contextlib import asynccontextmanager
from contextvars import ContextVar, Token
from functools import wraps
from typing import Any, AsyncGenerator, Awaitable, Callable, Dict, List, Optional, Sequence, Type, TypeVar

from rap.client.endpoint import BaseEndpoint, BaseEndpointProvider, LocalEndpointProvider, Picker
from rap.client.model import Response
from rap.client.processor.base import BaseClientProcessor, chain_processor
from rap.client.transport.pool import PoolProvider
from rap.client.transport.transport import Transport, TransportProvider
from rap.client.types import CLIENT_EVENT_FN
from rap.common.cache import Cache
from rap.common.channel import UserChannelCovariantType, get_corresponding_channel_class
from rap.common.collect_statistics import WindowStatistics
from rap.common.types import T_ParamSpec as P
from rap.common.types import T_ReturnType as R_T
from rap.common.utils import EventEnum, get_func_sig

__all__ = ["BaseClient", "Client"]
CHANNEL_F = Callable[[UserChannelCovariantType], Awaitable[None]]
Callable_T = TypeVar("Callable_T")


class _PickStub(object):
    def __init__(self, transport: Transport):
        self._transport: Transport = transport

    def __call__(self, *args, **kwargs) -> "_PickStub":
        return self

    async def __aenter__(self) -> Transport:
        return self._transport

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        return None


_pick_stub_context: ContextVar[Optional[_PickStub]] = ContextVar("_pick_stub_context", default=None)


class BaseClient:
    """Human-friendly `rap client` api"""

    def __init__(
        self,
        cache: Optional[Cache] = None,
        window_statistics: Optional[WindowStatistics] = None,
        through_deadline: bool = False,
        pool_provider: Optional[PoolProvider] = None,
        transport_provider: Optional[TransportProvider] = None,
        endpoint_provider: Optional[BaseEndpointProvider] = None,
    ):
        """
        :param cache: rap.common.cache.Cache
        :param window_statistics: rap.common.collect_statistics.WindowStatistics
        :param through_deadline: enable through deadline param to server
        """
        self._processor: Optional[BaseClientProcessor] = None
        self._through_deadline: bool = through_deadline
        self._event_dict: Dict[EventEnum, List[CLIENT_EVENT_FN]] = {
            value: [] for value in EventEnum.__members__.values()
        }
        self._cache: Cache = cache or Cache()
        self._window_statistics: WindowStatistics = window_statistics or WindowStatistics()

        ep: BaseEndpointProvider = endpoint_provider or LocalEndpointProvider.build({"ip": "localhost", "port": 9000})
        self._tp = transport_provider or TransportProvider.build()

        # Implementing dependency injection manually
        ep.inject((pool_provider or PoolProvider.build()).inject(self._tp))
        self._endpoint: BaseEndpoint = ep.create_instance()

        self.register_event_handler(EventEnum.before_start, lambda _: self._window_statistics.statistics_data())
        self.register_event_handler(EventEnum.after_end, lambda _: self._window_statistics.close())

    @property
    def endpoint(self) -> BaseEndpoint:
        return self._endpoint

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
        await self._run_event_handler(EventEnum.before_end)
        await self.endpoint.stop()
        await self._run_event_handler(EventEnum.after_end)

    async def start(self) -> None:
        """Create client transport"""
        await self._run_event_handler(EventEnum.before_start)
        await self.endpoint.start()
        await self._run_event_handler(EventEnum.after_start)

    async def await_start(self) -> None:
        """Wait for client startup to complete"""
        await self.endpoint.await_start()

    @property
    def is_close(self) -> bool:
        """Whether the client is closed"""
        return self._endpoint.is_close

    def _check_run(self) -> None:
        if not self.is_close:
            raise RuntimeError("The method cannot be called on the run")

    ########################
    # client event handler #
    ########################
    def register_event_handler(self, event_name: EventEnum, fn: CLIENT_EVENT_FN) -> None:
        """Registering client event callbacks"""
        self._check_run()
        self._event_dict[event_name].append(fn)

    async def _run_event_handler(self, event_name: EventEnum) -> None:
        """Execute client event callbacks"""
        for handler in self._event_dict[event_name]:
            ret: Any = handler(self)  # type: ignore
            if asyncio.iscoroutine(ret):
                await ret  # type: ignore

    ############################
    # client processor handler #
    ############################
    def load_processor(self, *processor_list: BaseClientProcessor) -> None:
        self._check_run()
        if not processor_list:
            return
        for processor in processor_list:
            processor.app = self  # type: ignore
            for event_type, handle in processor.event_dict.items():
                self._event_dict[event_type].extend(handle)
        if self._processor:
            self._processor = chain_processor(self._processor, *processor_list)
        else:
            self._processor = chain_processor(*processor_list)
        self._tp.inject(self._processor)

    @property
    def processor(self) -> Optional[BaseClientProcessor]:
        return self._processor

    ####################
    # wrapper func api #
    ####################
    def _wrapper_func(
        self,
        func: Callable[P, R_T],  # type: ignore
        group: Optional[str],
        header: Optional[dict] = None,
        picker_class: Optional[Type[Picker]] = None,
        name: str = "",
    ) -> Callable[P, Awaitable[R_T]]:  # type: ignore
        """Decorate normal function"""
        name = name if name else func.__name__
        func_sig: inspect.Signature = get_func_sig(func)

        # if not inspect.iscoroutinefunction(func):
        #     raise TypeError(f"func:{func.__name__} must coroutine function")

        @wraps(func)  # type: ignore
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R_T:  # type: ignore
            # Mypy Can not know ParamSpecArgs like Sequence and ParamSpecKwargs like Dict
            _args: Sequence = args  # type: ignore
            _kwargs: Dict = kwargs  # type: ignore

            _header = _kwargs.pop("header", header)
            _picker_class = _kwargs.pop("picker_class", picker_class)
            through_deadline = _kwargs.pop("through_deadline", None)
            result: Any = await self.invoke_by_name(
                name,
                param=func_sig.bind(*_args, **_kwargs).arguments,
                group=group,
                header=_header,
                picker_class=_picker_class,
                through_deadline=through_deadline,
            )
            return result

        return wrapper

    def _wrapper_gen_func(
        self,
        func: Callable[P, R_T],  # type: ignore
        group: Optional[str],
        picker_class: Optional[Type[Picker]] = None,
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

            _picker_class = _kwargs.pop("picker_class", picker_class)
            async with (_pick_stub_context.get() or self.endpoint.picker(picker_class=_picker_class)) as transport:
                async with transport.channel(name, group) as channel:
                    await channel.write(func_sig.bind(*_args, **_kwargs).arguments)
                    async for result in channel.get_read_channel():
                        yield result

        return wrapper

    def _wrapper_channel(
        self,
        func: CHANNEL_F,
        group: Optional[str],
        name: str = "",
        metadata: Optional[dict] = None,
        picker_class: Optional[Type[Picker]] = None,
    ) -> Callable[..., Awaitable[None]]:
        """Decoration channel function"""
        name = name if name else func.__name__
        get_corresponding_channel_class(func)

        @wraps(func)
        async def wrapper() -> None:
            async with (_pick_stub_context.get() or self.endpoint.picker(picker_class=picker_class)) as transport:
                async with transport.channel(name, group, metadata=metadata) as channel:
                    await func(channel.get_user_channel_from_func(func))

        return wrapper

    #####################
    # register func api #
    #####################
    def register_func(
        self,
        name: str = "",
        group: Optional[str] = None,
        picker_class: Optional[Type[Picker]] = None,
    ) -> Callable[[Callable[P, R_T]], Callable[P, Awaitable[R_T]]]:  # type: ignore
        """Using this method to decorate a fake function can help you use it better.

        :param name: rap func name
        :param group: func's group, default value is `default`
        :param picker_class: Specific implementation of picker
        """

        def wrapper(func: Callable[P, R_T]) -> Callable[P, Awaitable[R_T]]:  # type: ignore
            return self._wrapper_func(func, group=group, name=name, picker_class=picker_class)

        return wrapper

    def register_gen_func(
        self,
        name: str = "",
        group: Optional[str] = None,
        picker_class: Optional[Type[Picker]] = None,
    ) -> Callable[[Callable[P, R_T]], Callable[P, AsyncGenerator[R_T, None]]]:  # type: ignore
        """Using this method to decorate a fake function can help you use it better.

        :param name: rap func name
        :param group: func's group, default value is `default`
        :param picker_class: Specific implementation of picker
        """

        def wrapper(func: Callable[P, R_T]) -> Callable[P, AsyncGenerator[R_T, None]]:  # type: ignore
            return self._wrapper_gen_func(func, group=group, name=name, picker_class=picker_class)

        return wrapper

    def register_channel(
        self,
        name: str = "",
        group: Optional[str] = None,
        picker_class: Optional[Type[Picker]] = None,
        metadata: Optional[dict] = None,
    ) -> Callable[[CHANNEL_F], Any]:
        """Using this method to decorate a fake function can help you use it better.

        :param name: rap func name
        :param group: func's group, default value is `default`
        :param picker_class: Specific implementation of picker
        :param metadata:
            Create the channel phase to synchronize the metadata of the server channel,
            which can be obtained during the channel
        """

        def wrapper(func: CHANNEL_F) -> Any:  # type: ignore
            return self._wrapper_channel(func, group=group, name=name, picker_class=picker_class, metadata=metadata)

        return wrapper

    def register(
        self,
        name: str = "",
        group: Optional[str] = None,
        picker_class: Optional[Type[Picker]] = None,
    ) -> Callable:
        """Using this method to decorate a fake function can help you use it better.
        (such as ide completion, ide reconstruction and type hints)
        and will be automatically registered according to the function type

        Warning: This method does not work well with Type Hints and is not recommended.

        :param name: rap func name
        :param group: func's group, default value is `default`
        :param picker_class: Specific implementation of picker
        """

        def wrapper(func: Callable[P, R_T]) -> Callable:  # type: ignore
            if inspect.iscoroutinefunction(func):
                return self._wrapper_func(func, group, name=name, picker_class=picker_class)  # type: ignore
            elif inspect.isasyncgenfunction(func):
                return self._wrapper_gen_func(func, group, name=name, picker_class=picker_class)  # type: ignore
            raise TypeError(f"func:{func.__name__} must coroutine function or async gen function")

        return wrapper

    ###################
    # client base api #
    ###################
    @asynccontextmanager
    async def fixed_transport(self, picker_class: Optional[Type[Picker]] = None) -> AsyncGenerator[_PickStub, None]:
        async with self.endpoint.picker(picker_class=picker_class) as transport:
            _pick_stub: _PickStub = _PickStub(transport)
            token: Token = _pick_stub_context.set(_pick_stub)
            yield _pick_stub
            _pick_stub_context.reset(token)

    async def request(
        self,
        name: str,
        param: Optional[dict] = None,
        header: Optional[dict] = None,
        group: Optional[str] = None,
        picker_class: Optional[Type[Picker]] = None,
        through_deadline: Optional[bool] = None,
    ) -> Response:
        """rpc client base invoke method
        Note: This method does not support parameter type checking, not support channels;

        :param name: rpc func name
        :param param: rpc func param
        :param group: func's group
        :param header: request header
        :param picker_class: Specific implementation of picker
        :param through_deadline: Whether the transparent transmission deadline
        """
        async with (_pick_stub_context.get() or self.endpoint.picker(picker_class=picker_class)) as transport:
            return await transport.request(name, param, group=group, header=header, through_deadline=through_deadline)

    async def invoke_by_name(
        self,
        name: str,
        param: Optional[dict] = None,
        header: Optional[dict] = None,
        group: Optional[str] = None,
        picker_class: Optional[Type[Picker]] = None,
        through_deadline: Optional[bool] = None,
    ) -> Any:
        """rpc client base invoke method
        Note: This method does not support parameter type checking, not support channels;

        :param name: rpc func name
        :param param: rpc func param
        :param group: func's group
        :param header: request header
        :param picker_class: Specific implementation of picker
        :param through_deadline: Whether the transparent transmission deadline
        """
        response: Response = await self.request(
            name, param, group=group, header=header, picker_class=picker_class, through_deadline=through_deadline
        )
        return response.body

    def invoke(
        self,
        func: Callable[P, R_T],  # type: ignore
        header: Optional[dict] = None,
        group: Optional[str] = None,
        picker_class: Optional[Type[Picker]] = None,
    ) -> Callable[P, Awaitable[R_T]]:  # type: ignore
        """automatically resolve function names and call invoke_by_name

        :param func: python func
        :param group: func's group, default value is `default`
        :param header: request header
        :param picker_class: Specific implementation of picker
        """
        return self._wrapper_func(func, group=group, header=header, picker_class=picker_class)

    def invoke_iterator(
        self,
        func: Callable[P, R_T],  # type: ignore
        group: Optional[str] = None,
        picker_class: Optional[Type[Picker]] = None,
    ) -> Callable[P, AsyncGenerator[R_T, None]]:  # type: ignore
        """Python-specific generator invoke

        :param func: python func
        :param group: func's group, default value is `default`
        :param picker_class: Specific implementation of picker
        """
        return self._wrapper_gen_func(func, group=group, picker_class=picker_class)

    def invoke_channel(
        self,
        func: Callable[[UserChannelCovariantType], Awaitable[None]],
        group: Optional[str] = None,
        picker_class: Optional[Type[Picker]] = None,
        metadata: Optional[dict] = None,
    ) -> Callable[..., Awaitable[None]]:
        """invoke channel fun

        :param func: python func
        :param group: func's group, default value is `default`
        :param picker_class: Specific implementation of picker
        :param metadata:
            Create the channel phase to synchronize the metadata of the server channel,
            which can be obtained during the channel
        """
        return self._wrapper_channel(func, group=group, picker_class=picker_class, metadata=metadata)

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
    ...
