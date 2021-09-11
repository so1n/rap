import asyncio
import inspect
import logging
import math
import random
import time
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Sequence, Tuple, Type, Union

from rap.client.model import Request, Response
from rap.client.processor.base import BaseProcessor
from rap.client.transport.channel import Channel
from rap.client.utils import get_exc_status_code_dict, raise_rap_error
from rap.common import event
from rap.common import exceptions as rap_exc
from rap.common.asyncio_helper import as_first_completed, safe_del_future
from rap.common.conn import Connection
from rap.common.exceptions import RPCError
from rap.common.snowflake import get_snowflake_id
from rap.common.types import SERVER_BASE_MSG_TYPE
from rap.common.utils import Constant

if TYPE_CHECKING:
    from rap.client.core import BaseClient
__all__ = ["Transport"]


class Transport(object):
    """base client transport, encapsulation of custom transport protocol"""

    _decay_time: float = 600.0

    def __init__(
        self,
        app: "BaseClient",
    ):
        self.app: "BaseClient" = app
        self._process_request_list: List = []
        self._process_response_list: List = []
        self._process_exception_list: List = []

        self._max_msg_id: int = 65535
        self._msg_id: int = random.randrange(self._max_msg_id)
        self._exc_status_code_dict: Dict[int, Type[rap_exc.BaseRapError]] = get_exc_status_code_dict()
        self._resp_future_dict: Dict[str, asyncio.Future[Response]] = {}
        self._channel_queue_dict: Dict[str, asyncio.Queue[Union[Response, Exception]]] = {}

        self._event_handle_dict: Dict[str, List[Callable[[Response], None]]] = {}

        for event_name in dir(event):
            event_class: Type[event.Event] = getattr(event, event_name)
            if inspect.isclass(event_class) and issubclass(event_class, event.Event) and event_class is not event.Event:
                self._event_handle_dict[event_class.event_name] = []

    def register_event_handle(self, event_name: Type[event.Event], fn: Callable[[Response], None]) -> None:
        """register recv response event callback handle
        :param event_name: event name
        :param fn: event callback
        """
        if event_name not in self._event_handle_dict:
            raise KeyError(f"{event_name}")
        if fn in self._event_handle_dict[event_name.event_name]:
            raise ValueError(f"{fn} already exists {event_name}")
        self._event_handle_dict[event_name.event_name].append(fn)

    def unregister_event_handle(self, event_name: Type[event.Event], fn: Callable[[Response], None]) -> None:
        """unregister recv response event callback handle
        :param event_name: event name
        :param fn: event callback
        """
        if event_name not in self._event_handle_dict:
            raise KeyError(f"{event_name}")
        self._event_handle_dict[event_name.event_name].remove(fn)

    async def _put_exc_to_receiver(self, response: Response, put_exc: Exception, correlation_id: str) -> None:
        """handler exc by response
        :param response: server response
        :param put_exc: get response exc
        :param correlation_id: consumer correlation id
        """
        for process_exc in self._process_exception_list:
            response, put_exc = await process_exc(response, put_exc)

        # send exc to consumer
        if correlation_id in self._channel_queue_dict:
            self._channel_queue_dict[correlation_id].put_nowait(put_exc)
        elif correlation_id in self._resp_future_dict:
            self._resp_future_dict[correlation_id].set_exception(put_exc)
        elif isinstance(put_exc, rap_exc.ServerError):
            raise put_exc
        else:
            logging.error(f"recv error msg:{response}, ignore")

    # flake8: noqa: C901
    async def dispatch_resp_from_conn(self, conn: Connection) -> None:
        """Distribute the response data to different consumers according to different conditions"""
        response, exc = await self._get_response_from_conn(conn)
        if not response:
            # not response data, not a legal response
            logging.error(str(exc))
            return

        correlation_id: str = f"{conn.sock_tuple}:{response.correlation_id}"
        if exc:
            await self._put_exc_to_receiver(response, exc, correlation_id)
        elif response.msg_type == Constant.SERVER_ERROR_RESPONSE or response.status_code in self._exc_status_code_dict:
            # gen and put rap error by response
            exc_class: Type["rap_exc.BaseRapError"] = self._exc_status_code_dict.get(
                response.status_code, rap_exc.BaseRapError
            )
            await self._put_exc_to_receiver(response, exc_class(response.body), correlation_id)
        elif response.msg_type == Constant.SERVER_EVENT:
            # customer event handle
            if response.func_name in self._event_handle_dict:
                event_handle_list: List[Callable[[Response], None]] = self._event_handle_dict[response.func_name]
                for fn in event_handle_list:
                    try:
                        fn(response)
                    except Exception as e:
                        logging.exception(f"run event name:{response.func_name} raise error:{e}")
            # server event msg handle
            if response.func_name == Constant.EVENT_CLOSE_CONN:
                # server want to close...do not send data
                logging.error(f"recv close conn event, event info:{response.body}")
                conn.available = False
            elif response.func_name in (Constant.DECLARE, Constant.PONG_EVENT):
                self._resp_future_dict[correlation_id].set_result(response)
            elif response.func_name == Constant.PING_EVENT:
                await self.write_to_conn(Request.from_event(self.app, event.PongEvent("")), conn)
            else:
                logging.error(f"recv not support event {response}")
        elif response.msg_type == Constant.CHANNEL_RESPONSE:
            # put msg to channel
            if correlation_id not in self._channel_queue_dict:
                logging.error(f"recv channel msg, but channel not create. channel id:{correlation_id}")
            else:
                self._channel_queue_dict[correlation_id].put_nowait(response)
        elif response.msg_type == Constant.MSG_RESPONSE and correlation_id in self._resp_future_dict:
            # set msg to future_dict's `future`
            self._resp_future_dict[correlation_id].set_result(response)
        else:
            logging.error(f"Can' parse response: {response}, ignore")
        return

    async def _get_response_from_conn(self, conn: Connection) -> Tuple[Optional[Response], Optional[Exception]]:
        """recv server msg and gen response handle"""
        try:
            response_msg: Optional[SERVER_BASE_MSG_TYPE] = await conn.read()
            logging.debug("recv raw data: %s", response_msg)
        except asyncio.TimeoutError as e:
            logging.error(f"recv response from {conn.connection_info} timeout")
            raise e

        if response_msg is None:
            raise ConnectionError("Connection has been closed")

        # parse response
        try:
            response: Response = Response.from_msg(self.app, conn, response_msg)
        except Exception as e:
            e_msg: str = f"recv wrong response:{response_msg}, ignore error:{e}"
            return None, rap_exc.ProtocolError(e_msg)

        # Legal response, but may be intercepted by the processor
        exc: Optional[Exception] = None
        try:
            for process_response in self._process_response_list:
                response = await process_response(response)
        except Exception as e:
            exc = e

        return response, exc

    async def declare(self, conn: Connection, timeout: Optional[int] = None) -> None:
        """
        After conn is initialized, connect to the server and initialize the connection resources.
        Only include server_name and get conn id two functions, if you need to expand the function,
          you need to process the request and response of the declared life cycle through the processor
        """
        try:
            response: Response = await self._base_request(
                Request.from_event(self.app, event.DeclareEvent({"server_name": self.app.server_name})),
                conn,
                timeout=timeout,
            )
        except asyncio.TimeoutError:
            raise asyncio.TimeoutError(f"conn:{conn} declare timeout")

        exc: Exception = ConnectionError(f"conn:{conn} declare error")
        if response.msg_type != Constant.SERVER_EVENT:
            raise exc
        elif response.func_name == Constant.DECLARE:
            if response.body.get("result", False) and "conn_id" in response.body:
                conn.conn_id = response.body["conn_id"]
                return
        raise exc

    async def ping(self, conn: Connection, timeout: int) -> None:
        start_time: float = time.time()
        mos: int = 5
        rtt: float = 0

        async def _ping() -> None:
            nonlocal mos
            nonlocal rtt
            response: Response = await self._base_request(
                Request.from_event(self.app, event.PingEvent({})), conn, timeout=timeout * 2
            )
            rtt += time.time() - start_time
            mos += response.body.get("mos", 10)

        await asyncio.wait_for(asyncio.gather(*[_ping(), _ping(), _ping()]), timeout=timeout)
        mos = mos // 3
        rtt = rtt / 3

        # declare
        now_time: float = time.time()
        old_last_ping_timestamp: float = conn.last_ping_timestamp
        old_rtt: float = conn.rtt
        old_mos: int = conn.mos

        # ewma
        td: float = now_time - old_last_ping_timestamp
        w: float = math.exp(-td / self._decay_time)

        if rtt < 0:
            rtt = 0
        if old_rtt <= 0:
            w = 0

        conn.rtt = old_rtt * w + rtt * (1 - w)
        conn.mos = int(old_mos * w + mos * (1 - w))
        conn.last_ping_timestamp = now_time
        conn.score = (conn.weight * mos) / conn.rtt

    ####################################
    # base one by one request response #
    ####################################
    async def _base_request(self, request: Request, conn: Connection, timeout: Optional[int] = None) -> Response:
        """Send data to the server and get the response from the server.
        :param request: client request obj
        :param conn: client conn
        :param timeout: recv response timeout

        :return: return server response
        """
        if not timeout:
            timeout = 9
        if not request.correlation_id:
            request.correlation_id = str(get_snowflake_id())
        resp_future_id: str = f"{conn.sock_tuple}:{request.correlation_id}"
        try:
            response_future: asyncio.Future[Response] = asyncio.Future()
            self._resp_future_dict[resp_future_id] = response_future
            await self.write_to_conn(request, conn)
            try:
                response: Response = await as_first_completed(
                    [asyncio.wait_for(response_future, timeout)],
                    not_cancel_future_list=[conn.conn_future],
                )
                response.state = request.state
                return response
            except asyncio.TimeoutError:
                raise asyncio.TimeoutError(f"msg_id:{resp_future_id} request timeout")
        finally:
            pop_future: Optional[asyncio.Future] = self._resp_future_dict.pop(resp_future_id, None)
            if pop_future:
                safe_del_future(pop_future)

    @staticmethod
    def before_write_handle(request: Request) -> None:
        """check header value"""

        def set_header_value(header_key: str, header_value: Any, is_cover: bool = False) -> None:
            """if key not in header, set header value"""
            if is_cover or header_key not in request.header:
                request.header[header_key] = header_value

        set_header_value("version", Constant.VERSION, is_cover=True)
        set_header_value("user_agent", Constant.USER_AGENT, is_cover=True)
        set_header_value("request_id", str(get_snowflake_id()), is_cover=True)

    #######################
    # base write_to_conn&read api #
    #######################
    async def write_to_conn(self, request: Request, conn: Connection) -> None:
        """gen msg_id and seng msg to conn"""
        request.conn = conn
        request.header["host"] = conn.peer_tuple
        msg_id: int = self._msg_id + 1
        # Avoid too big numbers
        self._msg_id = msg_id & self._max_msg_id

        self.before_write_handle(request)

        for process_request in self._process_request_list:
            await process_request(request)

        await conn.write((msg_id, *request.to_msg()))

    ######################
    # one by one request #
    ######################
    async def request(
        self,
        func_name: str,
        conn: Connection,
        arg_param: Optional[Sequence[Any]] = None,
        call_id: Optional[int] = None,
        group: Optional[str] = None,
        header: Optional[dict] = None,
        timeout: Optional[int] = None,
    ) -> Response:
        """msg request handle
        :param func_name: rpc func name
        :param conn: can write msg conn
        :param arg_param: rpc func param
        :param call_id: server gen func next id
        :param group: func's group
        :param header: request header
        :param timeout: request timeout
        """
        group = group or Constant.DEFAULT_GROUP
        call_id = call_id or -1
        arg_param = arg_param or []
        request: Request = Request(
            self.app,
            Constant.MSG_REQUEST,
            f"{self.app.server_name}/{group}/{func_name}",
            {"call_id": call_id, "param": arg_param},
            correlation_id=str(get_snowflake_id()),
        )
        if header:
            request.header.update(header)
        response: Response = await self._base_request(request, conn, timeout=timeout)
        if response.msg_type != Constant.MSG_RESPONSE:
            raise RPCError(f"response num must:{Constant.MSG_RESPONSE} not {response.msg_type}")
        if "exc" in response.body:
            exc_info: str = response.body.get("exc_info", "")
            if response.header.get("user_agent") == Constant.USER_AGENT:
                raise_rap_error(response.body["exc"], exc_info)
            else:
                raise rap_exc.RpcRunTimeError(exc_info)
        return response

    def channel(self, func_name: str, conn: Connection, group: Optional[str] = None) -> "Channel":
        """create and init channel
        :param func_name: rpc func name
        :param conn: channel transport conn
        :param group: func's group
        """
        target: str = f"/{group or Constant.DEFAULT_GROUP}/{func_name}"
        channel: Channel = Channel(self, target, conn)  # type: ignore
        correlation_id: str = f"{conn.sock_tuple}:{channel.channel_id}"
        self._channel_queue_dict[correlation_id] = channel.queue
        channel.channel_conn_future.add_done_callback(lambda f: self._channel_queue_dict.pop(correlation_id, None))
        return channel

    #############
    # processor #
    #############
    def load_processor(self, processor_list: List[BaseProcessor]) -> None:
        """load client processor"""
        for processor in processor_list:
            self._process_request_list.append(processor.process_request)
            self._process_response_list.append(processor.process_response)
            self._process_exception_list.append(processor.process_exc)

    def remove_processor(self, processor_list: List[BaseProcessor]) -> None:
        for processor in processor_list:
            self._process_request_list.remove(processor.process_request)
            self._process_response_list.remove(processor.process_response)
            self._process_exception_list.remove(processor.process_exc)
