import asyncio
import logging
import signal
import ssl
import threading
import time
from typing import Any, Callable, Coroutine, Dict, List, Optional, Set, Type

from rap.common import event
from rap.common.conn import ServerConnection
from rap.common.exceptions import ServerError
from rap.common.state import WindowState
from rap.common.types import BASE_MSG_TYPE, READER_TYPE, WRITER_TYPE
from rap.common.utils import EventEnum, RapFunc
from rap.server.context import context
from rap.server.model import Request, Response
from rap.server.plugin.middleware.base import BaseConnMiddleware, BaseMiddleware
from rap.server.plugin.processor.base import BaseProcessor
from rap.server.receiver import Receiver
from rap.server.registry import RegistryManager
from rap.server.sender import Sender
from rap.server.types import SERVER_EVENT_FN

__all__ = ["Server"]


class Server(object):
    def __init__(
        self,
        server_name: str,
        host: str = "localhost",
        port: int = 9000,
        timeout: int = 9,
        keep_alive: int = 1200,
        run_timeout: int = 9,
        ping_fail_cnt: int = 2,
        ping_sleep_time: int = 60,
        backlog: int = 1024,
        close_timeout: int = 9,
        ssl_crt_path: Optional[str] = None,
        ssl_key_path: Optional[str] = None,
        middleware_list: List[BaseMiddleware] = None,
        processor_list: List[BaseProcessor] = None,
        window_state: Optional[WindowState] = None,
    ):
        self.server_name: str = server_name
        self.host: str = host
        self.port: int = port
        self._timeout: int = timeout
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
        self._request_event_handle_dict: Dict[str, List[Callable[[Request], None]]] = {}

        self._depend_set: Set[Any] = set()  # Check whether any components have been re-introduced
        if middleware_list:
            self.load_middleware(middleware_list)
        if processor_list:
            self.load_processor(processor_list)

        self.registry: RegistryManager = RegistryManager()
        self.window_state: Optional[WindowState] = window_state
        if self.window_state and self.window_state.is_closed:
            self.register_server_event(EventEnum.before_start, self.window_state.change_state)

    def register_server_event(self, event_type: EventEnum, *event_handle_list: SERVER_EVENT_FN) -> None:
        for event_handle in event_handle_list:
            if (event_type, event_handle) not in self._depend_set:
                self._depend_set.add((event_type, event_handle))
                self._server_event_dict[event_type].append(event_handle)
            else:
                raise ImportError(f"even type:{event_type}, handle:{event_handle} already load")

    def register_request_event_handle(self, event_class: Type[event.Event], fn: Callable[[Request], None]) -> None:
        if event_class not in self._request_event_handle_dict:
            raise KeyError(f"{event_class}")
        if fn in self._request_event_handle_dict[event_class.event_name]:
            raise ValueError(f"{fn} already exists {event_class}")
        self._request_event_handle_dict[event_class.event_name].append(fn)

    def unregister_request_event_handle(self, event_class: Type[event.Event], fn: Callable[[Request], None]) -> None:
        if event_class not in self._request_event_handle_dict:
            raise KeyError(f"{event_class}")
        self._request_event_handle_dict[event_class.event_name].remove(fn)

    def load_middleware(self, middleware_list: List[BaseMiddleware]) -> None:
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
        if isinstance(func, RapFunc):
            func = func.raw_func

        self.registry.register(func, name, group=group, is_private=is_private, doc=doc)

    @property
    def is_closed(self) -> bool:
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
                    logging.exception(f"server event<{event_type}:{callback}> run error:{e}")

    async def create_server(self) -> "Server":
        if not self.is_closed:
            raise RuntimeError("Server status is running...")
        await self.run_event_list(EventEnum.before_start, is_raise=True)
        self._server = await asyncio.start_server(
            self.conn_handle, self.host, self.port, ssl=self._ssl_context, backlog=self._backlog
        )
        logging.info(f"server running on {self.host}:{self.port}. use ssl:{bool(self._ssl_context)}")
        await self.run_event_list(EventEnum.after_start)

        # fix different loop event
        self._run_event.clear()
        self._run_event = asyncio.Event()
        return self

    async def run_forever(self) -> None:
        if self.is_closed:
            await self.create_server()

        def _shutdown(signum: int, frame: Any) -> None:
            logging.debug("Receive signal %s, run shutdown...", signum)
            asyncio.ensure_future(self.shutdown())

        if threading.current_thread() is not threading.main_thread():
            logging.error("Signals can only be listened to from the main thread.")
        else:
            try:
                # only use in unix
                loop = asyncio.get_event_loop()
                for sig in [signal.SIGINT, signal.SIGTERM]:
                    loop.add_signal_handler(sig, _shutdown, sig, None)
            except NotImplementedError:
                for sig in [signal.SIGINT, signal.SIGTERM]:
                    signal.signal(sig, _shutdown)
        await self._run_event.wait()

    async def shutdown(self) -> None:
        if self.is_closed:
            return

        await self.run_event_list(EventEnum.before_end)

        # Notify the client that the server is ready to shut down
        async def send_shutdown_event(_conn: ServerConnection) -> None:
            # conn may be closed
            if not _conn.is_closed():
                try:
                    await Sender(_conn, self._timeout, processor_list=self._processor_list).send_event(
                        event.ShutdownEvent({"close_timeout": self._close_timeout})
                    )
                except ConnectionError:
                    # conn may be closed
                    pass

        task_list: List[Coroutine] = [send_shutdown_event(conn) for conn in self._connected_set if not conn.is_closed()]
        await asyncio.gather(*task_list)

        # Stop accepting new connections.
        if self._server:
            self._server.close()
            await self._server.wait_closed()

        # until connections close
        logging.info("Waiting for connections to close. (CTRL+C to force quit)")
        close_timestamp: int = int(time.time()) + self._close_timeout
        while self._connected_set and close_timestamp > int(time.time()):
            await asyncio.sleep(0.1)

        self._run_event.set()
        await self.run_event_list(EventEnum.after_end, is_raise=True)

    async def conn_handle(self, reader: READER_TYPE, writer: WRITER_TYPE) -> None:
        conn: ServerConnection = ServerConnection(reader, writer, self._timeout)
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
        sender: Sender = Sender(conn, self._timeout, processor_list=self._processor_list)
        receiver: Receiver = Receiver(
            self,  # type: ignore
            conn,
            self._run_timeout,
            sender,
            self._ping_fail_cnt,
            self._ping_sleep_time,
            self._request_event_handle_dict,
            processor_list=self._processor_list,
        )
        recv_msg_handle_future_set: Set[asyncio.Future] = set()

        async def recv_msg_handle(_request_msg: Optional[BASE_MSG_TYPE]) -> None:
            if _request_msg is None:
                await sender.send_event(event.CloseConnEvent("request is empty"))
                return
            try:
                request: Request = Request.from_msg(_request_msg, conn)
            except Exception as closer_e:
                logging.error(f"{conn.peer_tuple} send bad msg:{_request_msg}, error:{closer_e}")
                await sender.send_event(event.CloseConnEvent("protocol error"))
                await conn.await_close()
                return

            with context as c:
                c.request = request
                try:
                    response: Optional[Response] = await receiver.dispatch(request)
                    await sender(response)
                except Exception as closer_e:
                    logging.exception(f"raw_request handle error e")
                    await sender.send_exc(ServerError(str(closer_e)))

        while not conn.is_closed():
            try:
                request_msg: Optional[BASE_MSG_TYPE] = await conn.read(self._keep_alive)
                # create future handle msg
                future: asyncio.Future = asyncio.ensure_future(recv_msg_handle(request_msg))
                future.add_done_callback(lambda f: recv_msg_handle_future_set.remove(f))
                recv_msg_handle_future_set.add(future)

            except asyncio.TimeoutError:
                logging.error(f"recv data from {conn.peer_tuple} timeout. close conn")
                await sender.send_event(event.CloseConnEvent("keep alive timeout"))
                break
            except IOError:
                break
            except Exception as e:
                logging.error(f"recv data from {conn.peer_tuple} error:{e}, conn has been closed")

        if recv_msg_handle_future_set:
            logging.debug("wait recv msg handle future")
            while len(recv_msg_handle_future_set) > 0:
                await asyncio.sleep(0.1)
        receiver.del_receiver()
        if not conn.is_closed():
            conn.close()
            logging.debug(f"close connection: %s", conn.peer_tuple)
