import asyncio
import logging
import math
import random
import sys
import time
from collections import deque
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, AsyncGenerator, Callable, Deque, Dict, List, Optional, Sequence, Tuple, Type

from rap.client.model import ClientContext, Request, Response
from rap.client.processor.base import belong_to_base_method
from rap.client.transport.channel import Channel
from rap.client.utils import get_exc_status_code_dict, raise_rap_error
from rap.common import event
from rap.common import exceptions as rap_exc
from rap.common.asyncio_helper import (
    Deadline,
    IgnoreDeadlineTimeoutExc,
    Semaphore,
    TaskGroup,
    TaskGroupExc,
    as_first_completed,
    deadline_context,
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

    _max_correlation_id: int = 65535
    _correlation_id: int = 1

    _score: float = 10.0

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
        min_ping_interval: Optional[int] = None,
        max_ping_interval: Optional[int] = None,
        ping_fail_cnt: Optional[int] = None,
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

        self._exc_status_code_dict: Dict[int, Type[rap_exc.BaseRapError]] = get_exc_status_code_dict()
        self._resp_future_dict: Dict[int, asyncio.Future[Response]] = {}
        self._channel_queue_dict: Dict[int, asyncio.Queue[Response]] = {}

        self.host: str = host
        self.port: int = port
        self.weight: int = weight
        self._ssl_crt_path: Optional[str] = ssl_crt_path
        self._pack_param: Optional[dict] = pack_param
        self._unpack_param: Optional[dict] = unpack_param
        self._max_inflight: Optional[int] = max_inflight

        self._semaphore: Semaphore = Semaphore(max_inflight or 100)
        self._context_dict: Dict[int, ClientContext] = {}

        # If True, it means that conn needs to be closed after processing the request
        self._close_soon_flag: bool = False

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
        self._min_ping_interval: int = min_ping_interval or 1
        self._max_ping_interval: int = max_ping_interval or 3
        self._ping_fail_cnt: int = ping_fail_cnt or 3
        self._ping_fn_list: List[Callable[["Transport"], None]] = []
        self.inflight_load: Deque[int] = deque([0, 0, 0, 0, 0], maxlen=5)  # save history inflight(like Linux load)
        self._last_ping_timestamp: float = time.time()
        self._rtt: float = 0.0

        self._client_info: InmutableDict = InmutableDict(
            host=self._conn.peer_tuple,
            version=constant.VERSION,
            user_agent=constant.USER_AGENT,
        )
        self._server_info: InmutableDict = InmutableDict()

    def _broadcast_server_event(self, response: Response) -> None:
        """Spread the response to all transport"""
        for _, future in self._resp_future_dict.items():
            future.set_result(response)
        for _, queue in self._channel_queue_dict.items():
            queue.put_nowait(response)

    @asynccontextmanager
    async def _transport_context(self, c_id: Optional[int] = None) -> AsyncGenerator[ClientContext, None]:
        """
        Create the request context

        It should be noted that when the context is obtained,
        it means that conn will be used, even if no request is sent at this time.
        So you need to call this method as close as possible to the place where the request is sent

        :param c_id: The ID corresponding to the context object, if it is empty, an ID is automatically generated
        """
        if not self.available:
            raise RuntimeError("transport has been marked as unavailable and cannot create new requests")
        if not c_id:
            correlation_id: int = self._correlation_id + 2
            # Avoid too big numbers
            self._correlation_id = correlation_id & self._max_correlation_id
        else:
            correlation_id: int = c_id
        context: ClientContext = ClientContext()
        context.app = self.app
        context.correlation_id = correlation_id
        context.conn = self._conn
        context.server_info = self._server_info
        context.client_info = self._client_info

        # create context
        await self._semaphore.acquire()
        self._context_dict[correlation_id] = context
        for on_context_enter in self.on_context_enter_processor_list:
            await on_context_enter(context)

        # any code
        exc_type, exc_val, exc_tb = None, None, None
        try:
            yield context
        except Exception as e:
            exc_type, exc_val, exc_tb = sys.exc_info()
            raise e
        finally:
            # release context
            for on_context_exit in reversed(self.on_context_exit_processor_list):
                await on_context_exit(context, exc_type, exc_val, exc_tb)
            self._semaphore.release()
            self._context_dict.pop(correlation_id, None)

            if self._close_soon_flag and self._semaphore.inflight == self._semaphore.raw_value:
                await self.await_close()

    async def _ping_handle(self) -> None:
        ping_fail_interval: int = int(self._max_ping_interval * self._ping_fail_cnt)

        def _change_transport_available() -> None:
            now_time: float = time.time()
            available: bool = (now_time - self._last_ping_timestamp) < ping_fail_interval
            logger.debug("transport:%s available:%s _rtt:%s", self.connection_info, available, self._rtt)
            if not available:
                raise RuntimeError(f"ping {self.sock_tuple} timeout... exit")
            else:
                self.inflight_load.append(int(self.inflight / self.raw_inflight * 100))
                # Simple design, don't want to use pandas&numpy in the app framework
                median_inflight: int = sorted(self.inflight_load)[len(self.inflight_load) // 2 + 1]
                if median_inflight < 20:
                    # When transport is just created, inflight is 5.
                    # The current transport is not handling too many requests and needs to lower its priority
                    self.available_level -= 1
                elif self.available and self.available_level < 5:
                    # If the current transport load is moderate and the
                    # priority is not optimal (priority less than 5), then increase its priority.
                    self.available_level += 1

            if self.available_level <= 0 and self.available:
                # The current transport availability level is 0, which means that it is not available
                raise RuntimeError("The current transport is idle and needs to be closed")

        try:
            while True:
                # If no event loop is running, should just exit
                get_event_loop()
                logger.debug("transport:%s available:%s _rtt:%s", self.peer_tuple, self.available_level, self._rtt)
                _change_transport_available()
                next_ping_interval: int = random.randint(self._min_ping_interval, self._max_ping_interval)
                try:
                    with Deadline(next_ping_interval, timeout_exc=IgnoreDeadlineTimeoutExc()) as d:
                        for fn in self._ping_fn_list:
                            fn(self)
                        await self.ping()
                        await self.sleep_and_listen(d.surplus)
                except asyncio.CancelledError:
                    return
                except Exception as e:
                    logger.debug(f"{self.connection_info} ping event error:{e}")
        finally:
            # Arrange for a graceful shutdown of the transport
            self.grace_close()

    async def _listen_conn(self) -> None:
        """listen server msg from transport"""
        logger.debug("conn:%s listen:%s start", self._conn.sock_tuple, self._conn.peer_tuple)
        try:
            async with TaskGroup() as tg:
                while not self._conn.is_closed():
                    try:
                        response_msg: Optional[SERVER_BASE_MSG_TYPE] = await asyncio.wait_for(
                            self._conn.read(), timeout=self._read_timeout
                        )
                    except asyncio.TimeoutError as e:
                        logger.error(f"conn:{self._conn.sock_tuple} recv response from {self._conn.peer_tuple} timeout")
                        raise e

                    if not response_msg:
                        raise ConnectionError("Connection has been closed")
                    if response_msg[0] == constant.SERVER_EVENT:
                        tg.create_task(self._server_send_msg_handler(response_msg))
                    else:
                        tg.create_task(self._server_recv_msg_handler(response_msg))
        except Exception as e:
            if isinstance(e, TaskGroupExc):
                e = e.error[0]
            if not isinstance(e, (asyncio.CancelledError, CloseConnException)):
                self._conn.set_reader_exc(e)
                logger.exception(f"conn:{self._conn.sock_tuple} listen {self._conn.peer_tuple} error:{e}")
                if not self._conn.is_closed():
                    await self._conn.await_close()

    @property
    def pick_score(self) -> float:
        """
        Combine the scores obtained through the server side and the usage of the client side to calculate the score
        """
        return self._score * (1 - (self._semaphore.inflight / self._semaphore.raw_value)) * self.available_level

    @property
    def inflight(self) -> int:
        """Get the number of conn in use"""
        return self._semaphore.inflight

    @property
    def raw_inflight(self) -> int:
        return self._semaphore.raw_value

    ######################
    # proxy _conn feature #
    ######################
    def add_close_callback(self, fn: Callable[[asyncio.Future], None]):
        self._conn.conn_future.add_done_callback(fn)

    def remove_close_callback(self, fn: Callable[[asyncio.Future], None]):
        self._conn.conn_future.remove_done_callback(fn)

    async def sleep_and_listen(self, delay: float) -> None:
        """like asyncio.sleep, but closing conn will terminate the hibernation"""
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
        elif self.available_level > 0:
            self._available = True

    ############
    # base api #
    ############
    def add_ping_callback(self, fn: Callable[["Transport"], None]):
        self._ping_fn_list.append(fn)

    def remove_ping_callback(self, fn: Callable[["Transport"], None]):
        self._ping_fn_list.remove(fn)

    def is_closed(self) -> bool:
        return self._conn.is_closed()

    def close(self) -> None:
        if self.is_closed():
            return
        self.available = False
        self._conn.close()

    def grace_close(self) -> None:
        """Graceful shutdown, ensuring that closing the transport does not affect requests being processed"""
        if self.is_closed():
            return
        self.available = False
        if not self._semaphore.inflight:
            # If no request is being processed, arrange to close conn as soon as possible,
            # otherwise it will wait until the request is processed before closing conn
            get_event_loop().call_soon(self.close)
        else:
            self._close_soon_flag = True

    async def await_close(self) -> None:
        self.close()
        await self._conn.await_close()

    async def connect(self) -> None:
        if not self.is_closed():
            return
        await self._conn.connect()
        self.available_level = 5
        self._close_soon_flag = False
        listen_future = asyncio.ensure_future(self._listen_conn())
        ping_future = asyncio.ensure_future(self._ping_handle())
        self.add_close_callback(lambda _: safe_del_future(listen_future))
        self.add_close_callback(lambda _: safe_del_future(ping_future))

    ####################
    # response handler #
    ####################
    async def _get_response(self, response_msg: SERVER_BASE_MSG_TYPE, context: ClientContext) -> Optional[Response]:
        """Generate Response object through response msg"""
        try:
            response: Response = Response.from_msg(msg=response_msg, context=context)
        except Exception as e:
            logger.exception(f"recv wrong response:{response_msg}, ignore error:{e}")
            return None

        if response.msg_type == constant.SERVER_ERROR_RESPONSE or response.status_code in self._exc_status_code_dict:
            # Generate rap standard error
            exc_class: Type["rap_exc.BaseRapError"] = self._exc_status_code_dict.get(
                response.status_code, rap_exc.BaseRapError
            )
            response.exc = exc_class.build(response.body)
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

    async def _server_recv_msg_handler(self, response_msg: SERVER_BASE_MSG_TYPE) -> None:
        correlation_id: int = response_msg[1]
        context: Optional[ClientContext] = self._context_dict.get(correlation_id, None)
        if not context:
            logger.error(f"Can not found context from {correlation_id}, {response_msg}")
            return
        response = await self._get_response(response_msg=response_msg, context=context)
        if not response:
            return
        if correlation_id in self._resp_future_dict:
            self._resp_future_dict[correlation_id].set_result(response)
        elif correlation_id in self._channel_queue_dict:
            self._channel_queue_dict[correlation_id].put_nowait(response)
        else:
            logger.error(f"Can not dispatch response: {response}, ignore")
        return

    async def _server_send_msg_handler(self, response_msg: SERVER_BASE_MSG_TYPE) -> None:
        correlation_id: int = response_msg[1]
        async with self._transport_context(c_id=correlation_id) as context:
            response = await self._get_response(response_msg=response_msg, context=context)
            if not response:
                return
            # server event msg handle
            if response.func_name == constant.EVENT_CLOSE_CONN:
                # server want to close...do not send data
                logger.info(f"recv close transport event, event info:{response.body}")
                self.grace_close()
                exc = CloseConnException(response.body)
                response.exc = exc
                self._broadcast_server_event(response)
                self._conn.set_reader_exc(exc)
            elif response.func_name == constant.PING_EVENT:
                request: Request = Request.from_event(event.PingEvent(""), context)
                request.msg_type = constant.SERVER_EVENT

                with get_deadline(3):
                    await self.write_to_conn(request)
            elif response.func_name.endswith(event.ShutdownEvent.event_name):
                # server want to close...do not create request
                logger.info(f"recv shutdown event, event info:{response.body}")
                self.grace_close()
            else:
                logger.error(f"recv not support event response:{response}")

    ##########################################
    # rap interactive protocol encapsulation #
    ##########################################
    async def declare(self) -> None:
        """
        After transport is initialized, connect to the server and initialize the connection resources.
        Only include server_name and get transport id two functions, if you need to expand the function,
          you need to process the request and response of the declared life cycle through the processor
        """
        if self._server_info:
            raise RuntimeError("declare can not be called twice")
        async with self._transport_context() as context:
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
            async with self._transport_context() as context:
                response: Response = await self._base_request(Request.from_event(event.PingEvent({}), context))
            rtt += time.time() - start_time
            mos += response.body.get("mos", 5)

        await asyncio.gather(*[_ping() for _ in range(cnt)])
        mos = get_value_by_range(mos // cnt, 0, 5)
        rtt = get_value_by_range(rtt / cnt, 0)

        # declare
        now_time: float = time.time()
        old_last_ping_timestamp: float = self._last_ping_timestamp
        old_rtt: float = self._rtt
        old_mos: int = self.mos

        # ewma
        td: float = now_time - old_last_ping_timestamp
        w: float = get_value_by_range(math.exp(-td / self._decay_time), 0)

        self._rtt = old_rtt * w + rtt * (1 - w)
        self.mos = int(old_mos * w + mos * (1 - w))
        self._last_ping_timestamp = now_time
        self._score = (self.weight * mos) / self._rtt

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
        async with self._transport_context() as context:
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

        async with self._transport_context() as context:
            correlation_id: int = context.correlation_id
            channel: Channel = Channel(
                transport=self, target=target, channel_id=correlation_id, context=context  # type: ignore
            )
            self._channel_queue_dict[correlation_id] = channel.queue
            channel.channel_conn_future.add_done_callback(lambda f: self._channel_queue_dict.pop(correlation_id, None))

            async with channel as channel:
                yield channel
