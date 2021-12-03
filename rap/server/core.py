import asyncio
import logging
import signal
import ssl
import threading
from typing import Any, Awaitable, Callable, Coroutine, Dict, List, Optional, Set

from rap.common import event
from rap.common.asyncio_helper import Deadline
from rap.common.cache import Cache
from rap.common.collect_statistics import WindowStatistics
from rap.common.conn import CloseConnException, ServerConnection
from rap.common.exceptions import ServerError
from rap.common.signal_broadcast import add_signal_handler, remove_signal_handler
from rap.common.snowflake import async_get_snowflake_id
from rap.common.types import BASE_MSG_TYPE, READER_TYPE, WRITER_TYPE
from rap.common.utils import EventEnum, RapFunc
from rap.server.model import Request, Response
from rap.server.plugin.middleware.base import BaseConnMiddleware, BaseMiddleware
from rap.server.plugin.processor.base import BaseProcessor
from rap.server.receiver import Receiver
from rap.server.registry import FuncModel, RegistryManager
from rap.server.sender import Sender
from rap.server.types import SERVER_EVENT_FN

__all__ = ["Server"]
logger: logging.Logger = logging.getLogger(__name__)


class Server(object):
    def __init__(
        self,
        server_name: str,
        host: str = "localhost",
        port: int = 9000,
        send_timeout: int = 9,
        keep_alive: int = 1200,
        run_timeout: int = 9,
        ping_fail_cnt: int = 2,
        ping_sleep_time: int = 60,
        backlog: int = 1024,
        close_timeout: int = 9,
        ssl_crt_path: Optional[str] = None,
        ssl_key_path: Optional[str] = None,
        pack_param: Optional[dict] = None,
        unpack_param: Optional[dict] = None,
        middleware_list: List[BaseMiddleware] = None,
        processor_list: List[BaseProcessor] = None,
        call_func_permission_fn: Optional[Callable[[Request], Awaitable[FuncModel]]] = None,
        window_statistics: Optional[WindowStatistics] = None,
        cache_interval: Optional[float] = None,
    ):
        """
        :param server_name: server name
        :param host: listen host
        :param port: listen port
        :param send_timeout: send msg timeout
        :param keep_alive: conn keep_alive time
        :param run_timeout: Maximum execution time per call
        :param ping_fail_cnt: When ping fails continuously and exceeds this value, conn will be disconnected
        :param ping_sleep_time: ping message interval time
        :param backlog: server backlog
        :param close_timeout: The maximum time to wait for conn to process messages when shutting down the service
        :param ssl_crt_path: ssl crt path
        :param ssl_key_path: ssl key path
        :param pack_param: msgpack.Pack param
        :param unpack_param: msgpack.UnPack param
        :param middleware_list: Server middleware list
        :param processor_list: Server processor list
        :param call_func_permission_fn: Check the permission to call the function
        :param window_statistics: Server window state
        :param cache_interval: Server cache interval seconds to clean up expired data
        """
        self.server_name: str = server_name
        self.host: str = host
        self.port: int = port
        self._send_timeout: int = send_timeout
        self._run_timeout: int = run_timeout
        self._close_timeout: int = close_timeout
        self._keep_alive: int = keep_alive
        self._backlog: int = backlog
        self._ping_fail_cnt: int = ping_fail_cnt
        self._ping_sleep_time: int = ping_sleep_time
        self._server: Optional[asyncio.AbstractServer] = None
        self._connected_set: Set[ServerConnection] = set()
        self._run_event: asyncio.Event = asyncio.Event()
        self._run_event.set()

        self._pack_param: Optional[dict] = pack_param
        self._unpack_param: Optional[dict] = unpack_param
        self._ssl_context: Optional[ssl.SSLContext] = None
        if ssl_crt_path and ssl_key_path:
            self._ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
            self._ssl_context.check_hostname = False
            self._ssl_context.load_cert_chain(ssl_crt_path, ssl_key_path)

        self._middleware_list: List[BaseMiddleware] = []
        self._processor_list: List[BaseProcessor] = []
        self._server_event_dict: Dict[EventEnum, List[SERVER_EVENT_FN]] = {
            value: [] for value in EventEnum.__members__.values()
        }

        self._depend_set: Set[Any] = set()  # Check whether any components have been re-introduced
        if middleware_list:
            self.load_middleware(middleware_list)
        if processor_list:
            self.load_processor(processor_list)

        self._call_func_permission_fn: Optional[Callable[[Request], Awaitable[FuncModel]]] = call_func_permission_fn
        self.registry: RegistryManager = RegistryManager()
        self.cache: Cache = Cache(interval=cache_interval)
        self.window_statistics: WindowStatistics = window_statistics or WindowStatistics(interval=60)
        if self.window_statistics is not None and self.window_statistics.is_closed:
            self.register_server_event(EventEnum.before_start, lambda _app: self.window_statistics.statistics_data())

    def register_server_event(self, event_enum: EventEnum, *event_handle_list: SERVER_EVENT_FN) -> None:
        """register server event handler
        :param event_enum: server event
        :param event_handle_list: event handler list
        """
        for event_handle in event_handle_list:
            if (event_enum, event_handle) not in self._depend_set:
                self._depend_set.add((event_enum, event_handle))
                self._server_event_dict[event_enum].append(event_handle)
            else:
                raise ImportError(f"even type:{event_enum}, handle:{event_handle} already load")

    def load_middleware(self, middleware_list: List[BaseMiddleware]) -> None:
        """load server middleware
        :param middleware_list: server middleware list
        """
        for middleware in middleware_list:
            if middleware not in self._depend_set:
                self._depend_set.add(middleware)
            else:
                raise ImportError(f"{middleware} middleware already load")

            middleware.app = self  # type: ignore
            if isinstance(middleware, BaseConnMiddleware):
                middleware.load_sub_middleware(self._conn_handle)
                setattr(self, self._conn_handle.__name__, middleware)
            elif isinstance(middleware, BaseMiddleware):
                self._middleware_list.append(middleware)
            else:
                raise RuntimeError(f"{middleware} must instance of {BaseMiddleware}")

            for event_type, server_event_handle_list in middleware.server_event_dict.items():
                self.register_server_event(event_type, *server_event_handle_list)

    def load_processor(self, processor_list: List[BaseProcessor]) -> None:
        """load server processor
        :param processor_list server load processor
        """
        for processor in processor_list:
            if processor not in self._depend_set:
                self._depend_set.add(processor)
            else:
                raise ImportError(f"{processor} processor already load")
            if isinstance(processor, BaseProcessor):
                processor.app = self  # type: ignore
                self._processor_list.append(processor)
            else:
                raise RuntimeError(f"{processor} must instance of {BaseProcessor}")

            for event_type, server_event_handle_list in processor.server_event_dict.items():
                self.register_server_event(event_type, *server_event_handle_list)

    def register(
        self,
        func: Callable,
        name: Optional[str] = None,
        group: Optional[str] = None,
        is_private: bool = False,
        doc: Optional[str] = None,
    ) -> None:
        """Register function with Server
        :param func: function
        :param name: The real name of the function in the server
        :param group: The group of the function
        :param is_private: Whether the function is private or not, in general,
          private functions are only allowed to be called by the local client,
          but rap does not impose any mandatory restrictions
        :param doc: Describe what the function does
        """
        if isinstance(func, RapFunc):
            func = func.raw_func

        self.registry.register(func, name, group=group, is_private=is_private, doc=doc)

    @property
    def is_closed(self) -> bool:
        """Whether the service is closed"""
        return self._run_event.is_set()

    async def run_event_list(self, event_type: EventEnum, is_raise: bool = False) -> None:
        event_handle_list: Optional[List[SERVER_EVENT_FN]] = self._server_event_dict.get(event_type)
        if not event_handle_list:
            return
        for callback in event_handle_list:
            try:
                ret: Any = callback(self)  # type: ignore
                if asyncio.iscoroutine(ret):
                    await ret
            except Exception as e:
                if is_raise:
                    raise e
                else:
                    logger.exception(f"server event<{event_type}:{callback}> run error:{e}")

    async def create_server(self) -> "Server":
        """start server"""
        if not self.is_closed:
            raise RuntimeError("Server status is running...")
        await self.run_event_list(EventEnum.before_start, is_raise=True)
        self._server = await asyncio.start_server(
            self.conn_handle, self.host, self.port, ssl=self._ssl_context, backlog=self._backlog
        )
        logger.info(f"server running on {self.host}:{self.port}. use ssl:{bool(self._ssl_context)}")
        await self.run_event_list(EventEnum.after_start)

        # fix different loop event
        self._run_event.clear()
        self._run_event = asyncio.Event()
        return self

    async def run_forever(self) -> None:
        """Start the service and keep running until shutdown is called or received signal `int` or `term`"""
        if self.is_closed:
            await self.create_server()

        def _shutdown(signum: int, frame: Any) -> None:
            logger.debug("Receive signal %s, run shutdown...", signum)
            asyncio.ensure_future(self.shutdown())

        if threading.current_thread() is not threading.main_thread():
            raise RuntimeError("Signals can only be listened to from the main thread.")
        else:
            for sig in [signal.SIGINT, signal.SIGTERM]:
                add_signal_handler(sig, _shutdown)
            await self._run_event.wait()
            for sig in [signal.SIGINT, signal.SIGTERM]:
                remove_signal_handler(sig, _shutdown)

    async def shutdown(self) -> None:
        """Notify the client that it is about to close, and the client should not send new messages at this time.
        The server no longer accepts the establishment of a new conn,
        and the server officially shuts down after waiting for the established conn to be closed.
        """
        if self.is_closed:
            return

        # Notify the client that the server is ready to shut down
        async def send_shutdown_event(_conn: ServerConnection) -> None:
            # conn may be closed
            if not _conn.is_closed():
                try:
                    await Sender(
                        self, _conn, self._send_timeout, processor_list=self._processor_list  # type: ignore
                    ).send_event(event.ShutdownEvent({"close_timeout": self._close_timeout}))
                except ConnectionError:
                    # conn may be closed
                    pass

        try:
            with Deadline(self._close_timeout):
                await self.run_event_list(EventEnum.before_end)

                # Stop accepting new connections.
                if self._server:
                    self._server.close()
                    await self._server.wait_closed()

                task_list: List[Coroutine] = [
                    send_shutdown_event(conn) for conn in self._connected_set if not conn.is_closed()
                ]
                if task_list:
                    logger.info("send shutdown event to client")
                await asyncio.gather(*task_list)

                # until connections close
                logger.info(f"{self} Waiting for connections to close. (CTRL+C to force quit)")
                while self._connected_set:
                    await asyncio.sleep(0.1)
                await self.run_event_list(EventEnum.after_end, is_raise=True)
        finally:
            self._run_event.set()

    async def conn_handle(self, reader: READER_TYPE, writer: WRITER_TYPE) -> None:
        """Handle initialization and recycling of conn"""
        conn: ServerConnection = ServerConnection(
            reader, writer, pack_param=self._pack_param, unpack_param=self._unpack_param
        )
        conn.conn_id = str(await async_get_snowflake_id())
        try:
            self._connected_set.add(conn)
            await self._conn_handle(conn)
            try:
                conn.conn_future.result()
            except Exception:
                pass
        finally:
            self._connected_set.remove(conn)

    async def _conn_handle(self, conn: ServerConnection) -> None:
        """Receive or send messages by conn"""
        sender: Sender = Sender(self, conn, self._send_timeout, processor_list=self._processor_list)  # type: ignore
        receiver: Receiver = Receiver(
            self,  # type: ignore
            conn,
            self._run_timeout,
            sender,
            self._ping_fail_cnt,
            self._ping_sleep_time,
            processor_list=self._processor_list,
            call_func_permission_fn=self._call_func_permission_fn,
        )
        recv_msg_handle_future_set: Set[asyncio.Future] = set()

        async def recv_msg_handle(_request_msg: Optional[BASE_MSG_TYPE]) -> None:
            if _request_msg is None:
                await sender.send_event(event.CloseConnEvent("request is empty"))
                return
            try:
                request: Request = Request.from_msg(self, _request_msg, conn)  # type: ignore
            except Exception as closer_e:
                logger.error(f"{conn.peer_tuple} send bad msg:{_request_msg}, error:{closer_e}")
                await sender.send_event(event.CloseConnEvent("protocol error"))
                await conn.await_close()
                return

            try:
                response: Optional[Response] = await receiver.dispatch(request)
                await sender(response)
            except Exception as closer_e:
                logging.exception("raw_request handle error e")
                await sender.send_exc(ServerError(str(closer_e)))

        while not conn.is_closed():
            try:
                with Deadline(self._keep_alive):
                    request_msg: Optional[BASE_MSG_TYPE] = await conn.read()
                # create future handle msg
                future: asyncio.Future = asyncio.ensure_future(recv_msg_handle(request_msg))
                future.add_done_callback(lambda f: recv_msg_handle_future_set.remove(f))
                recv_msg_handle_future_set.add(future)
            except asyncio.TimeoutError:
                logging.error(f"recv data from {conn.peer_tuple} timeout. close conn")
                await sender.send_event(event.CloseConnEvent("keep alive timeout"))
                break
            except (IOError, CloseConnException):
                break
            except Exception as e:
                logging.error(f"recv data from {conn.peer_tuple} error:{e}, conn has been closed")

        if recv_msg_handle_future_set:
            logging.debug("wait recv msg handle future")
            while len(recv_msg_handle_future_set) > 0:
                await asyncio.sleep(0.1)
        if not conn.is_closed():
            conn.close()
            logging.debug("close connection: %s", conn.peer_tuple)
