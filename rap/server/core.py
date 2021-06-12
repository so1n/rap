import asyncio
import inspect
import logging
import ssl
from contextvars import Token
from typing import Any, Callable, List, Optional, Set, Tuple, Union

from rap.common.conn import ServerConnection
from rap.common.event import CloseConnEvent
from rap.common.exceptions import ServerError
from rap.common.state import WindowState
from rap.common.types import BASE_REQUEST_TYPE, READER_TYPE, WRITER_TYPE
from rap.common.utils import Constant, RapFunc
from rap.server.context import rap_context
from rap.server.model import Request, Response
from rap.server.plugin.middleware.base import BaseConnMiddleware, BaseMiddleware
from rap.server.plugin.processor.base import BaseProcessor
from rap.server.receiver import Receiver
from rap.server.registry import RegistryManager
from rap.server.sender import Sender

__all__ = ["Server"]


class Server(object):
    def __init__(
        self,
        server_name: str,
        timeout: int = 9,
        keep_alive: int = 1200,
        run_timeout: int = 9,
        ping_fail_cnt: int = 2,
        ping_sleep_time: int = 60,
        backlog: int = 1024,
        ssl_crt_path: Optional[str] = None,
        ssl_key_path: Optional[str] = None,
        start_event_list: List[Callable] = None,
        stop_event_list: List[Callable] = None,
        middleware_list: List[BaseMiddleware] = None,
        processor_list: List[BaseProcessor] = None,
        window_state: Optional[WindowState] = None,
    ):
        self._host_list: List[Tuple[str, int]] = []
        self.server_name: str = server_name
        self._timeout: int = timeout
        self._run_timeout: int = run_timeout
        self._keep_alive: int = keep_alive
        self._backlog: int = backlog
        self._ping_fail_cnt: int = ping_fail_cnt
        self._ping_sleep_time: int = ping_sleep_time
        self._server_list: List[asyncio.AbstractServer] = []

        self._ssl_context: Optional[ssl.SSLContext] = None
        if ssl_crt_path and ssl_key_path:
            self._ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
            self._ssl_context.check_hostname = False
            self._ssl_context.load_cert_chain(ssl_crt_path, ssl_key_path)

        self._start_event_list: List[Callable] = []
        self._stop_event_list: List[Callable] = []
        self._middleware_list: List[BaseMiddleware] = []
        self._processor_list: List[BaseProcessor] = []
        self._depend_set: Set[Any] = set()  # Check whether any components have been re-introduced

        if start_event_list:
            self.load_start_event(start_event_list)
        if stop_event_list:
            self.load_stop_event(stop_event_list)
        if middleware_list:
            self.load_middleware(middleware_list)
        if processor_list:
            self.load_processor(processor_list)

        self.registry: RegistryManager = RegistryManager()
        self.window_state: Optional[WindowState] = window_state
        if self.window_state and self.window_state.is_closed:
            self.load_start_event([self.window_state.change_state])

    def _load_event(self, event_list: List[Callable], event: Callable) -> None:
        if not (inspect.isfunction(event) or asyncio.iscoroutine(event) or inspect.ismethod(event)):
            raise ImportError(f"{event} must be fun or coroutine, not {type(event)}")

        if event not in self._depend_set:
            self._depend_set.add(event)
        else:
            raise ImportError(f"{event} event already load")
        event_list.append(event)

    def load_start_event(self, event_list: List[Callable]) -> None:
        for event in event_list:
            self._load_event(self._start_event_list, event)

    def load_stop_event(self, event_list: List[Callable]) -> None:
        for event in event_list:
            self._load_event(self._stop_event_list, event)

    def load_middleware(self, middleware_list: List[BaseMiddleware]) -> None:
        for middleware in middleware_list:
            if middleware not in self._depend_set:
                self._depend_set.add(middleware)
            else:
                raise ImportError(f"{middleware} middleware already load")

            middleware.app = self  # type: ignore
            if isinstance(middleware, BaseConnMiddleware):
                middleware.load_sub_middleware(self._conn_handle)
                self._conn_handle = middleware  # type: ignore
            elif isinstance(middleware, BaseMiddleware):
                self._middleware_list.append(middleware)
            else:
                raise RuntimeError(f"{middleware} must instance of {BaseMiddleware}")

            self.load_start_event([middleware.start_event_handle])
            self.load_stop_event([middleware.stop_event_handle])

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

            self.load_start_event([processor.start_event_handle])
            self.load_stop_event([processor.stop_event_handle])

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

    def bind(self, local_ip: str = "localhost", port: int = 9000, host_ip: str = "") -> None:
        if not self.is_closed:
            raise RuntimeError("Server status is running...can not bind")
        self._host_list.append((local_ip, port))
        # TODO register center

    @property
    def is_closed(self) -> bool:
        return not bool(self._server_list)

    @staticmethod
    async def run_callback_list(callback_list: List[Callable]) -> None:
        if not callback_list:
            return
        for callback in callback_list:
            if asyncio.iscoroutine(callback) or asyncio.isfuture(callback) or asyncio.iscoroutinefunction(callback):
                await callback()
            else:
                callback()

    async def create_server(self) -> "Server":
        if not self._host_list:
            raise RuntimeError("Could not find the bind conn")
        if not self.is_closed:
            raise RuntimeError("Server status is running...")
        await self.run_callback_list(self._start_event_list)
        for host in self._host_list:
            ip, port = host
            self._server_list.append(
                await asyncio.start_server(self.conn_handle, ip, port, ssl=self._ssl_context, backlog=self._backlog)
            )
            logging.info(f"server running on {host}. use ssl:{bool(self._ssl_context)}")
        return self

    async def await_closed(self) -> None:
        for server in self._server_list:
            server.close()
            await server.wait_closed()
        await self.run_callback_list(self._stop_event_list)
        # NOTE: await bg future cancel or done
        await asyncio.sleep(0.1)

    async def conn_handle(self, reader: READER_TYPE, writer: WRITER_TYPE) -> None:
        conn: ServerConnection = ServerConnection(reader, writer, self._timeout)
        await self._conn_handle(conn)
        try:
            conn.conn_future.result()
        except Exception:
            pass

    async def _conn_handle(self, conn: ServerConnection) -> None:
        sender: Sender = Sender(conn, self._timeout, processor_list=self._processor_list)
        receiver: Receiver = Receiver(
            self,  # type: ignore
            conn,
            self._run_timeout,
            sender,
            self._ping_fail_cnt,
            self._ping_sleep_time,
            processor_list=self._processor_list,
        )

        async def recv_msg_handle(_request_msg: Optional[BASE_REQUEST_TYPE]) -> None:
            if _request_msg is None:
                await sender.send_event(CloseConnEvent("request is empty"))
                return
            try:
                request: Request = Request.from_msg(_request_msg, conn)
            except Exception as closer_e:
                logging.error(f"{conn.peer_tuple} send bad msg:{_request_msg}, error:{closer_e}")
                await sender.send_event(CloseConnEvent("protocol error"))
                await conn.await_close()
                return

            token: Token = rap_context.set({"request": request})
            try:
                response: Optional[Response] = await receiver.dispatch(request)
                await sender(response)
            except Exception as closer_e:
                logging.exception(f"raw_request handle error e")
                await sender.send_exc(ServerError(str(closer_e)))
            finally:
                rap_context.reset(token)

        while not conn.is_closed():
            try:
                request_msg: Optional[BASE_REQUEST_TYPE] = await conn.read(self._keep_alive)
                # create future handle msg
                asyncio.ensure_future(recv_msg_handle(request_msg))
            except asyncio.TimeoutError:
                logging.error(f"recv data from {conn.peer_tuple} timeout. close conn")
                await sender.send_event(CloseConnEvent("keep alive timeout"))
                break
            except IOError:
                break
            except Exception as e:
                logging.error(f"recv data from {conn.peer_tuple} error:{e}, conn has been closed")

        if not conn.is_closed():
            conn.close()
            logging.debug(f"close connection: %s", conn.peer_tuple)
