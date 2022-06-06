import asyncio
import logging
import math
import sys
import time
from collections import deque
from contextlib import asynccontextmanager
from types import TracebackType
from typing import TYPE_CHECKING, Any, AsyncGenerator, Callable, Deque, Dict, Optional, Sequence, Tuple, Type

from rap.client.model import ClientContext, Request, Response
from rap.client.processor.base import belong_to_base_method
from rap.client.transport.channel import Channel
from rap.client.utils import get_exc_status_code_dict, raise_rap_error
from rap.common import event
from rap.common import exceptions as rap_exc
from rap.common.asyncio_helper import (
    Deadline,
    Semaphore,
    as_first_completed,
    deadline_context,
    del_future,
    done_future,
    get_deadline,
    get_event_loop,
    safe_del_future,
)
from rap.common.conn import CloseConnException, Connection
from rap.common.exceptions import IgnoreNextProcessor, InvokeError, RPCError
from rap.common.number_range import NumberRange, get_value_by_range
from rap.common.types import SERVER_BASE_MSG_TYPE
from rap.common.utils import InmutableDict, constant

if TYPE_CHECKING:
    from rap.client.core import BaseClient
__all__ = ["Transport"]
logger: logging.Logger = logging.getLogger(__name__)


class Transport(object):
    """base client transport, encapsulation of custom transport protocol and proxy _conn feature"""

    _decay_time: float = 600.0
    weight: int = NumberRange.i(10, 0, 10)
    mos: int = NumberRange.i(5, 0, 5)

    # Mark whether transport is available,
    # if not, no new requests can be initiated (does not affect requests in use)
    _available: bool = False
    _available_level: int = NumberRange.i(0, 0, 5)

    def __init__(
        self,
        app: "BaseClient",
        host: str,
        port: int,
        weight: int,
        ssl_crt_path: Optional[str] = None,
        pack_param: Optional[dict] = None,
        unpack_param: Optional[dict] = None,
        max_inflight: Optional[int] = None,
        read_timeout: Optional[int] = None,
    ):
        self.app: "BaseClient" = app
        self._conn: Connection = Connection(
            host,
            port,
            pack_param=pack_param,
            unpack_param=unpack_param,
            ssl_crt_path=ssl_crt_path,
        )
        self._read_timeout = read_timeout or 1200

        self._max_correlation_id: int = 65535
        self._correlation_id: int = 1
        self._context_dict: Dict[int, ClientContext] = {}
        self._exc_status_code_dict: Dict[int, Type[rap_exc.BaseRapError]] = get_exc_status_code_dict()
        self._resp_future_dict: Dict[int, asyncio.Future[Response]] = {}
        self._channel_queue_dict: Dict[int, asyncio.Queue[Response]] = {}

        self.host: str = host
        self.port: int = port
        self.weight: int = weight
        self._ssl_crt_path: Optional[str] = ssl_crt_path
        self.score: float = 10.0

        self.listen_future: asyncio.Future = done_future()
        self.semaphore: Semaphore = Semaphore(max_inflight or 100)

        # processor
        self.on_context_enter_processor_list: Sequence[Callable] = [
            i.on_context_enter for i in self.app.processor_list if not belong_to_base_method(i.on_context_enter)
        ]
        self.on_context_exit_processor_list: Sequence[Callable] = [
            i.on_context_exit for i in self.app.processor_list if not belong_to_base_method(i.on_context_exit)
        ]
        self.process_request_processor_list: Sequence[Callable] = [
            i.process_request for i in self.app.processor_list if not belong_to_base_method(i.process_request)
        ]
        self.process_response_processor_list: Sequence[Callable] = [
            i.process_response for i in self.app.processor_list if not belong_to_base_method(i.process_response)
        ]
        self.process_exception_processor_list: Sequence[Callable] = [
            i.process_exc for i in self.app.processor_list if not belong_to_base_method(i.process_exc)
        ]

        # ping
        self.inflight_load: Deque[int] = deque(maxlen=5)  # save history inflight(like Linux load)
        self.ping_future: asyncio.Future = done_future()
        self.last_ping_timestamp: float = time.time()
        self.rtt: float = 0.0

        self._client_info: InmutableDict = InmutableDict(
            host=self._conn.peer_tuple,
            version=constant.VERSION,
            user_agent=constant.USER_AGENT,
        )
        self._server_info: InmutableDict = InmutableDict()

    @property
    def pick_score(self) -> float:
        # Combine the scores obtained through the server side and the usage of the client side to calculate the score
        return self.score * (1 - (self.semaphore.inflight / self.semaphore.raw_value))

    ######################
    # proxy _conn feature #
    ######################
    async def sleep_and_listen(self, delay: float) -> None:
        await self._conn.sleep_and_listen(delay)

    @property
    def conn_future(self) -> asyncio.Future:
        return self._conn.conn_future

    @property
    def connection_info(self) -> str:
        return self._conn.connection_info

    @property
    def peer_tuple(self) -> Tuple[str, int]:
        return self._conn.peer_tuple

    @property
    def sock_tuple(self) -> Tuple[str, int]:
        return self._conn.sock_tuple

    async def await_close(self) -> None:
        safe_del_future(self.ping_future)
        safe_del_future(self.listen_future)
        await self._conn.await_close()

    #################
    # available api #
    #################
    @property
    def available(self) -> bool:
        return self._available

    @available.setter
    def available(self, value: bool) -> None:
        self._available = value

    @property
    def available_level(self) -> int:
        return self._available_level

    @available_level.setter
    def available_level(self, value: int) -> None:
        self._available_level = value
        if self._available_level <= 0:
            self._available = False

    ############
    # base api #
    ############
    def is_closed(self) -> bool:
        return self._conn.is_closed() or self.listen_future.done()

    def close(self) -> None:
        safe_del_future(self.ping_future)
        del_future(self.listen_future)
        self._conn.close()

    def close_soon(self) -> None:
        get_event_loop().call_later(60, self.close)
        self.available = False

    async def connect(self) -> None:
        await self._conn.connect()
        self.available_level = 5
        self.available = True
        self.listen_future = asyncio.ensure_future(self.listen_conn())
        await self.declare()

    ##########################################
    # rap interactive protocol encapsulation #
    ##########################################
    async def process_response(self, response: Response) -> Response:
        """
        If this response is accompanied by an exception, handle the exception directly and throw it.
        """
        if not response.exc:
            try:
                for process_response in reversed(self.process_response_processor_list):
                    response = await process_response(response)
            except IgnoreNextProcessor:
                pass
            except Exception as e:
                response.exc = e
                response.tb = sys.exc_info()[2]
        if response.exc:
            for process_exception in reversed(self.process_exception_processor_list):
                raw_response: Response = response
                try:
                    response = await process_exception(response)  # type: ignore
                except IgnoreNextProcessor:
                    break
                except Exception as e:
                    logger.exception(
                        f"processor:{process_exception} handle response:{response.correlation_id} error:{e}"
                    )
                    response = raw_response
        return response

    async def listen_conn(self) -> None:
        """listen server msg from transport"""
        logger.debug("listen:%s start", self._conn.peer_tuple)
        try:
            while not self._conn.is_closed():
                await self.response_handler()
        except (asyncio.CancelledError, CloseConnException):
            pass
        except Exception as e:
            self._conn.set_reader_exc(e)
            logger.exception(f"listen {self._conn.connection_info} error:{e}")
            if not self._conn.is_closed():
                await self._conn.await_close()

    def _broadcast_server_event(self, response: Response) -> None:
        for _, future in self._resp_future_dict.items():
            future.set_result(response)
        for _, queue in self._channel_queue_dict.items():
            queue.put_nowait(response)

    # flake8: noqa: C901
    async def response_handler(self) -> None:
        """Distribute the response data to different consumers according to different conditions"""
        # read response msg
        try:
            response_msg: Optional[SERVER_BASE_MSG_TYPE] = await asyncio.wait_for(
                self._conn.read(), timeout=self._read_timeout
            )
            logger.debug("recv raw data: %s", response_msg)
        except asyncio.TimeoutError as e:
            logger.error(f"recv response from {self._conn.connection_info} timeout")
            raise e

        if response_msg is None:
            raise ConnectionError("Connection has been closed")
        # share state
        correlation_id: int = response_msg[1]
        context: Optional[ClientContext] = self._context_dict.get(correlation_id, None)
        if not context:
            if response_msg[0] == constant.SERVER_EVENT:
                context = ClientContext()
                context.app = self.app
                context.conn = self._conn
                context.correlation_id = correlation_id
                context.server_info = self._server_info
                context.client_info = self._client_info
            else:
                logger.error(f"Can not found context from {correlation_id}, {response_msg}")
                return
        # parse response
        try:
            response: Response = Response.from_msg(msg=response_msg, context=context)
        except Exception as e:
            logger.exception(f"recv wrong response:{response_msg}, ignore error:{e}")
            return

        if response.msg_type == constant.SERVER_ERROR_RESPONSE or response.status_code in self._exc_status_code_dict:
            # Generate rap standard error
            exc_class: Type["rap_exc.BaseRapError"] = self._exc_status_code_dict.get(
                response.status_code, rap_exc.BaseRapError
            )
            response.exc = exc_class.build(response.body)
        response = await self.process_response(response)
        # dispatch response
        if response.msg_type == constant.SERVER_EVENT:
            # server event msg handle
            if response.func_name == constant.EVENT_CLOSE_CONN:
                # server want to close...do not send data
                logger.info(f"recv close transport event, event info:{response.body}")
                self.available = False
                exc = CloseConnException(response.body)
                self._conn.set_reader_exc(exc)
                response.exc = exc
                self._broadcast_server_event(response)
            elif response.func_name == constant.PING_EVENT:
                request: Request = Request.from_event(event.PingEvent(""), context)
                request.msg_type = constant.SERVER_EVENT

                async def _send_server_event():
                    with get_deadline(3):
                        await self.write_to_conn(request)

                asyncio.create_task(_send_server_event())
            else:
                logger.error(f"recv not support event response:{response}")
        elif response.msg_type == constant.CLIENT_EVENT:
            if response.func_name in (constant.DECLARE, constant.PING_EVENT):
                if correlation_id in self._resp_future_dict:
                    self._resp_future_dict[correlation_id].set_result(response)
                else:
                    logger.error(f"recv event:{response.func_name}, but not handler")
            else:
                logger.error(f"recv not support event response:{response}")
        elif response.msg_type == constant.CHANNEL_RESPONSE and correlation_id in self._channel_queue_dict:
            # put msg to channel
            self._channel_queue_dict[correlation_id].put_nowait(response)
        elif response.msg_type == constant.MSG_RESPONSE and correlation_id in self._resp_future_dict:
            # set msg to future_dict's `future`
            self._resp_future_dict[correlation_id].set_result(response)
        else:
            logger.error(f"Can not dispatch response: {response}, ignore")
        return

    def context(self) -> "Any":
        transport: "Transport" = self

        class TransportContext(object):
            correlation_id: int

            def __init__(self) -> None:
                self.correlation_id = transport._gen_correlation_id()
                self.context: ClientContext = ClientContext()
                self.context.app = transport.app
                self.context.conn = transport._conn
                self.context.correlation_id = self.correlation_id
                self.context.server_info = transport._server_info
                self.context.client_info = transport._client_info
                transport._context_dict[self.correlation_id] = self.context
                super().__init__()

            async def __aenter__(self) -> "ClientContext":
                for on_context_enter in transport.on_context_enter_processor_list:
                    try:
                        await on_context_enter(self)
                    except Exception as e:
                        logger.exception(f"on_context_enter error:{e}")
                return self.context

            async def __aexit__(
                self,
                exc_type: Optional[Type[BaseException]],
                exc_val: Optional[BaseException],
                exc_tb: Optional[TracebackType],
            ) -> None:
                transport._context_dict.pop(self.correlation_id, None)
                for on_context_exit in reversed(transport.on_context_exit_processor_list):
                    try:
                        await on_context_exit(self.context, exc_type, exc_val, exc_tb)
                    except Exception as e:
                        logger.exception(f"on_context_exit error:{e}")

        return TransportContext()

    async def declare(self) -> None:
        """
        After transport is initialized, connect to the server and initialize the connection resources.
        Only include server_name and get transport id two functions, if you need to expand the function,
          you need to process the request and response of the declared life cycle through the processor
        """

        async with self.context() as context:
            response: Response = await self._base_request(
                Request.from_event(
                    event.DeclareEvent({"server_name": self.app.server_name, "client_info": context.client_info}),
                    context,
                )
            )

        if (
            response.msg_type == constant.CLIENT_EVENT
            and response.func_name == constant.DECLARE
            and response.body.get("result", False)
            and "conn_id" in response.body
        ):
            self._conn.conn_id = response.body["conn_id"]
            self._server_info = InmutableDict(response.body["server_info"])
            return
        raise ConnectionError(f"transport:{self._conn} declare error")

    async def ping(self, cnt: int = 3) -> None:
        """
        Send three requests to check the response time of the client and server.
        At the same time, obtain the current quality score of the server to help the client better realize automatic
         load balancing (if the server supports this function)
        :param cnt: ping cnt
        """
        start_time: float = time.time()
        mos: int = 5
        rtt: float = 0

        async def _ping() -> None:
            nonlocal mos
            nonlocal rtt
            async with self.context() as context:
                response: Response = await self._base_request(Request.from_event(event.PingEvent({}), context))
            rtt += time.time() - start_time
            mos += response.body.get("mos", 5)

        await asyncio.gather(*[_ping() for _ in range(cnt)])
        mos = get_value_by_range(mos // cnt, 0, 5)
        rtt = get_value_by_range(rtt / cnt, 0)

        # declare
        now_time: float = time.time()
        old_last_ping_timestamp: float = self.last_ping_timestamp
        old_rtt: float = self.rtt
        old_mos: int = self.mos

        # ewma
        td: float = now_time - old_last_ping_timestamp
        w: float = get_value_by_range(math.exp(-td / self._decay_time), 0)

        self.rtt = old_rtt * w + rtt * (1 - w)
        self.mos = int(old_mos * w + mos * (1 - w))
        self.last_ping_timestamp = now_time
        self.score = (self.weight * mos) / self.rtt

    ####################################
    # base one by one request response #
    ####################################
    async def _base_request(self, request: Request) -> Response:
        """Send data to the server and get the response from the server.
        :param request: client request obj

        :return: return server response
        """
        try:
            response_future: asyncio.Future[Response] = asyncio.Future()
            self._resp_future_dict[request.correlation_id] = response_future
            await self.write_to_conn(request)
            response: Response = await as_first_completed(
                [response_future],
                not_cancel_future_list=[self._conn.conn_future],
            )
            if response.exc:
                if isinstance(response.exc, InvokeError):
                    raise_rap_error(response.exc.exc_name, response.exc.exc_info)
                else:
                    raise response.exc
            return response
        finally:
            pop_future: Optional[asyncio.Future] = self._resp_future_dict.pop(request.correlation_id, None)
            if pop_future:
                safe_del_future(pop_future)

    def _gen_correlation_id(self) -> int:
        correlation_id: int = self._correlation_id + 2
        # Avoid too big numbers
        self._correlation_id = correlation_id & self._max_correlation_id
        return correlation_id

    ##########################
    # base write_to_conn api #
    ##########################
    async def write_to_conn(self, request: Request) -> None:
        """gen msg_id and seng msg to transport"""
        deadline: Optional[Deadline] = deadline_context.get()
        if self.app.through_deadline and deadline:
            request.header["X-rap-deadline"] = deadline.end_timestamp
        for process_request in self.process_request_processor_list:
            await process_request(request)
        await self._conn.write(request.to_msg())

    ######################
    # one by one request #
    ######################
    async def request(
        self,
        func_name: str,
        arg_param: Optional[Sequence[Any]] = None,
        group: Optional[str] = None,
        header: Optional[dict] = None,
    ) -> Response:
        """msg request handle
        :param func_name: rpc func name
        :param arg_param: rpc func param
        :param group: func's group
        :param header: request header
        """
        group = group or constant.DEFAULT_GROUP
        arg_param = arg_param or []
        async with self.context() as context:
            request: Request = Request(
                msg_type=constant.MSG_REQUEST,
                target=f"{self.app.server_name}/{group}/{func_name}",
                body=arg_param,
                context=context,
            )
            if header:
                request.header.update(header)
            response: Response = await self._base_request(request)
            if response.msg_type != constant.MSG_RESPONSE:
                raise RPCError(f"response num must:{constant.MSG_RESPONSE} not {response.msg_type}")
        return response

    @asynccontextmanager
    async def channel(self, func_name: str, group: Optional[str] = None) -> AsyncGenerator["Channel", None]:
        """create and init channel
        :param func_name: rpc func name
        :param group: func's group
        """
        target: str = f"/{group or constant.DEFAULT_GROUP}/{func_name}"

        async with self.context() as context:
            correlation_id: int = context.correlation_id
            channel: Channel = Channel(
                transport=self, target=target, channel_id=correlation_id, context=context  # type: ignore
            )
            self._channel_queue_dict[correlation_id] = channel.queue
            channel.channel_conn_future.add_done_callback(lambda f: self._channel_queue_dict.pop(correlation_id, None))

            async with channel as channel:
                yield channel
