import inspect
import sys
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, Sequence, Type

from rap.client.endpoint import BalanceEnum, BaseEndpoint, LocalEndpoint
from rap.client.model import Response
from rap.client.processor.base import BaseProcessor
from rap.client.transport.async_iterator import AsyncIteratorCall
from rap.client.transport.transport import Transport
from rap.client.types import CLIENT_EVENT_FN
from rap.common.cache import Cache
from rap.common.channel import UserChannel
from rap.common.collect_statistics import WindowStatistics
from rap.common.types import is_type
from rap.common.utils import EventEnum, RapFunc, param_handle

__all__ = ["BaseClient", "Client"]
CHANNEL_F = Callable[[UserChannel], Any]


class BaseClient:
    """Human-friendly `rap client` api"""

    endpoint: BaseEndpoint

    def __init__(
        self,
        server_name: str,
        keep_alive_timeout: Optional[int] = None,
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
        self.transport: Transport = Transport(self, read_timeout=keep_alive_timeout)  # type: ignore
        self._processor_list: List[BaseProcessor] = []
        self._through_deadline: bool = through_deadline
        self._event_dict: Dict[EventEnum, List[CLIENT_EVENT_FN]] = {
            value: [] for value in EventEnum.__members__.values()
        }
        self.cache: Cache = Cache(interval=cache_interval)
        self.window_statistics: WindowStatistics = WindowStatistics(
            interval=ws_min_interval, max_interval=ws_max_interval, statistics_interval=ws_statistics_interval
        )

    @property
    def through_deadline(self) -> bool:
        return self._through_deadline

    ##################
    # start & close #
    ################
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
        self.transport.load_processor(processor_list)

    #####################
    # register func api #
    #####################
    def _async_register(self, func: Callable, group: Optional[str], name: str = "") -> RapFunc:
        """Decorate normal function"""
        name = name if name else func.__name__
        func_sig: inspect.Signature = inspect.signature(func)
        return_type: Type = func_sig.return_annotation

        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            header: Optional[dict] = kwargs.pop("header", None)
            result: Any = await self.raw_invoke(name, param_handle(func_sig, args, kwargs), group=group, header=header)
            if not is_type(return_type, type(result)):
                raise RuntimeError(f"{func} return type is {return_type}, but result type is {type(result)}")
            return result

        return RapFunc(wrapper, func)

    def _async_gen_register(self, func: Callable, group: Optional[str], name: str = "") -> RapFunc:
        """Decoration generator function"""
        name = name if name else func.__name__
        func_sig: inspect.Signature = inspect.signature(func)
        return_type: Type = func_sig.return_annotation

        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            header: Optional[dict] = kwargs.pop("header", None)
            async with self.endpoint.picker() as conn:
                async for result in AsyncIteratorCall(
                    name,
                    self,  # type: ignore
                    conn,
                    param_handle(func_sig, args, kwargs),
                    group=group,
                    header=header,
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

    async def request(
        self,
        name: str,
        arg_param: Optional[Sequence[Any]] = None,
        header: Optional[dict] = None,
        group: Optional[str] = None,
    ) -> Response:
        """rpc client base invoke method
        Note: This method does not support parameter type checking, not support channels;
        :param name: rpc func name
        :param arg_param: rpc func param
        :param group: func's group
        :param header: request header
        """
        async with self.endpoint.picker() as conn:
            return await self.transport.request(name, conn, arg_param, group=group, header=header)

    async def raw_invoke(
        self,
        name: str,
        arg_param: Optional[Sequence[Any]] = None,
        header: Optional[dict] = None,
        group: Optional[str] = None,
    ) -> Any:
        """rpc client base invoke method
        Note: This method does not support parameter type checking, not support channels;
        :param name: rpc func name
        :param arg_param: rpc func param
        :param group: func's group
        :param header: request header
        """
        response: Response = await self.request(name, arg_param, group=group, header=header)
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
        :param func: python func
        :param arg_param: rpc func param
        :param kwarg_param: func kwargs param
        :param group: func's group, default value is `default`
        :param header: request header
        """
        if not arg_param:
            arg_param = ()
        if not kwarg_param:
            kwarg_param = {}
        return await self.raw_invoke(
            func.__name__, param_handle(inspect.signature(func), arg_param, kwarg_param), group=group, header=header
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
        :param func: python func
        :param arg_param: rpc func param
        :param kwarg_param: func kwargs param
        :param group: func's group, default value is `default`
        :param header: request header
        """
        if not inspect.isasyncgenfunction(func):
            raise TypeError("func must be async gen function")
        if not kwarg_param:
            kwarg_param = {}

        async with self.endpoint.picker() as conn:
            async for result in AsyncIteratorCall(
                func.__name__,
                self,  # type: ignore
                conn,
                param_handle(inspect.signature(func), arg_param, kwarg_param),
                header=header,
                group=group,
            ):
                yield result

    def inject(self, func: Callable, name: str = "", group: Optional[str] = None) -> RapFunc:
        """Replace the function with `RapFunc` and inject it into the client at the same time
        :param func: Python func
        :param name: rap func name
        :param group: func's group, default value is `default`
        """
        if isinstance(func, RapFunc):
            raise RuntimeError(f"{func.raw_func} already inject or register")
        new_func: RapFunc = self.register(name=name, group=group)(func)
        func_module: str = getattr(func, "__module__")
        if not func_module:
            raise AttributeError(f"{type(func)} object has no attribute '__module__'")
        sys.modules[func_module].__setattr__(func.__name__, new_func)
        return new_func

    @staticmethod
    def recovery(func: RapFunc) -> None:
        """Restore functions that have been injected"""
        if not isinstance(func, RapFunc):
            raise RuntimeError(f"{func} is not {RapFunc}, which can not recovery")
        func_module: str = getattr(func.raw_func, "__module__")
        if not func_module:
            raise AttributeError(f"{type(func)} object has no attribute '__module__'")
        sys.modules[func_module].__setattr__(func.__name__, func.raw_func)

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

        def wrapper(func: Callable) -> RapFunc:
            if not (inspect.iscoroutinefunction(func) or inspect.isasyncgenfunction(func)):
                raise TypeError(f"func:{func.__name__} must coroutine function or async gen function")
            func_sig: inspect.Signature = inspect.signature(func)
            func_arg_parameter: List[inspect.Parameter] = [
                i for i in func_sig.parameters.values() if i.default == i.empty
            ]
            if len(func_arg_parameter) == 1 and func_arg_parameter[0].annotation is UserChannel:
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
        keep_alive_timeout: read msg from conn timeout
        """

        super().__init__(
            server_name,
            keep_alive_timeout=keep_alive_timeout,
            cache_interval=cache_interval,
            ws_min_interval=ws_min_interval,
            ws_max_interval=ws_max_interval,
            ws_statistics_interval=ws_statistics_interval,
            through_deadline=through_deadline,
        )
        self.endpoint = LocalEndpoint(
            conn_list,
            self.transport,
            ssl_crt_path=ssl_crt_path,
            pack_param=None,
            unpack_param=None,
            balance_enum=select_conn_method,
            ping_fail_cnt=ping_fail_cnt,
            min_ping_interval=min_ping_interval,
            max_ping_interval=max_ping_interval,
            wait_server_recover=wait_server_recover,
        )
