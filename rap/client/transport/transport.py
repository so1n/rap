import asyncio
import inspect
import logging
import random
import time
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Sequence, Tuple, Type, Union

from rap.client.model import Request, Response
from rap.client.processor.base import BaseProcessor
from rap.client.transport.channel import Channel
from rap.client.utils import get_exc_status_code_dict, raise_rap_error
from rap.common import event
from rap.common import exceptions as rap_exc
from rap.common.conn import Connection
from rap.common.exceptions import RPCError
from rap.common.snowflake import get_snowflake_id
from rap.common.types import SERVER_BASE_MSG_TYPE
from rap.common.utils import Constant, as_first_completed

if TYPE_CHECKING:
    from rap.client.core import BaseClient
__all__ = ["Transport"]


class Transport(object):
    """base client transport, encapsulation of custom transport protocol"""

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

    async def ping_event(
        self,
        conn: Connection,
        ping_interval: Optional[int] = None,
        ping_fail_cnt: Optional[int] = None,
        wait_server_recover: bool = True,
    ) -> None:
        """client ping-pong handler, check conn is available
        :param conn: rap client conn
        :param ping_interval: send client ping interval, default 30
        :param ping_fail_cnt: How many times ping fails to judge as unavailable, default 3
        :param wait_server_recover: If False, ping failure will close conn
        """
        if not ping_interval:
            ping_interval = 10
        if not ping_fail_cnt:
            ping_fail_cnt = 3

        ping_fail_interval: int = ping_interval * ping_fail_cnt
        while True:
            diff_time: int = int(time.time()) - conn.last_ping_timestamp
            available: bool = diff_time < ping_fail_interval
            conn.available = available
            logging.debug("conn:%s available:%s RTT:%s", conn.peer_tuple, available, conn.RTT)
            if not available and not wait_server_recover:
                logging.error(f"ping {conn.sock_tuple} timeout... exit")
                return
            else:
                try:
                    await self.write_to_conn(Request.from_event(self.app, event.PingEvent({"time": time.time()})), conn)
                except Exception as e:
                    logging.debug(f"{conn} ping event exit.. error:{e}")

            await asyncio.wait_for(asyncio.shield(conn.listen_future), timeout=ping_interval)

    async def listen(self, conn: Connection) -> None:
        """listen server msg from conn"""
        logging.debug("listen:%s start", conn.peer_tuple)
        try:
            while not conn.is_closed():
                await self._dispatch_resp_from_conn(conn)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            conn.set_reader_exc(e)
            logging.exception(f"listen {conn.connection_info} error:{e}")
            if not conn.is_closed():
                await conn.await_close()

    async def _dispatch_resp_from_conn(self, conn: Connection) -> None:
        """Distribute the response data to different consumers according to different conditions"""
        response, exc = await self._read_from_conn(conn)
        if not response:
            # not response data, not a legal response
            logging.error(str(exc))
            return

        correlation_id: str = f"{conn.sock_tuple}:{response.correlation_id}"

        async def put_exc_to_receiver(put_response: Response, put_exc: Exception) -> None:
            # handler exc by response
            for process_exc in self._process_exception_list:
                put_response, put_exc = await process_exc(put_response, put_exc)

            # send exc to consumer
            if correlation_id in self._channel_queue_dict:
                self._channel_queue_dict[correlation_id].put_nowait(put_exc)
            elif correlation_id in self._resp_future_dict:
                self._resp_future_dict[correlation_id].set_exception(put_exc)
            elif isinstance(put_exc, rap_exc.ServerError):
                raise put_exc
            else:
                logging.error(f"recv error msg:{response}, ignore")

        if exc:
            await put_exc_to_receiver(response, exc)
            return
        elif response.msg_type == Constant.SERVER_ERROR_RESPONSE or response.status_code in self._exc_status_code_dict:
            # gen and put rap error by response
            exc_class: Type["rap_exc.BaseRapError"] = self._exc_status_code_dict.get(
                response.status_code, rap_exc.BaseRapError
            )
            await put_exc_to_receiver(response, exc_class(response.body))
            return
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
                event_exc: Exception = ConnectionError(f"recv close conn event, event info:{response.body}")
                raise event_exc
            elif response.func_name == Constant.PING_EVENT:
                await self.write_to_conn(Request.from_event(self.app, event.PongEvent("")), conn)
            elif response.func_name == Constant.PONG_EVENT:
                conn.last_ping_timestamp = int(time.time())
                conn.RTT = time.time() - response.body["time"]
                conn.server_cpu = response.body["server_cpu_percent"]
            elif response.func_name == Constant.DECLARE:
                if not response.body.get("result"):
                    raise rap_exc.AuthError("Declare error")
            else:
                logging.error(f"recv error event {response}")
            return
        elif response.msg_type == Constant.CHANNEL_RESPONSE:
            # put msg to channel
            if correlation_id not in self._channel_queue_dict:
                logging.error(f"recv channel msg, but channel not create. channel id:{correlation_id}")
            else:
                self._channel_queue_dict[correlation_id].put_nowait(response)
            return
        elif response.msg_type == Constant.MSG_RESPONSE and correlation_id in self._resp_future_dict:
            # set msg to future_dict's `future`
            self._resp_future_dict[correlation_id].set_result(response)
            return
        else:
            logging.error(f"Can' parse response: {response}, ignore")
            return

    async def read_from_conn(self, conn: Connection) -> Optional[Response]:
        """read response from conn, only use in declare lifecycle"""
        if conn.conn_id:
            raise rap_exc.RPCError("conn already declare, can not invoke read_from_conn")
        response, exc = await self._read_from_conn(conn)
        if exc:
            raise exc
        return response

    async def _read_from_conn(self, conn: Connection) -> Tuple[Optional[Response], Optional[Exception]]:
        """recv server msg handle"""
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

        # Legal response, but may be intercepted by the processer
        exc: Optional[Exception] = None
        try:
            for process_response in self._process_response_list:
                response = await process_response(response)
        except Exception as e:
            exc = e
        return response, exc

    async def declare(self, conn: Connection) -> None:
        """
        After conn is initialized, connect to the server and initialize the connection resources.
        Only include server_name and get conn id two functions, if you need to expand the function,
          you need to process the request and response of the declared life cycle through the processor
        """
        await self.write_to_conn(
            Request.from_event(self.app, event.DeclareEvent({"server_name": self.app.server_name})), conn
        )
        response: Optional[Response] = await self.read_from_conn(conn)

        exc: Exception = ConnectionError("create conn error")
        if not response or response.msg_type != Constant.SERVER_EVENT:
            raise exc
        elif response.func_name == Constant.EVENT_CLOSE_CONN:
            raise ConnectionError(f"recv close conn event, event info:{response.body}")
        elif response.func_name == Constant.DECLARE:
            if response.body.get("result", False) and "conn_id" in response.body:
                conn.conn_id = response.body["conn_id"]
                return
        raise exc

    ####################################
    # base one by one request response #
    ####################################
    async def _base_request(self, request: Request, conn: Connection, timeout: Optional[int] = 9) -> Response:
        """Send data to the server and get the response from the server.
        Will try to get the normal data through the conn list and return,
         if all conn calls fail(response.status_code >= 500), the exception generated by the last conn will be reported.
        """
        request.conn = conn
        resp_future_id: str = f"{conn.sock_tuple}:{request.correlation_id}"
        self._resp_future_dict[resp_future_id] = asyncio.Future()
        await self.write_to_conn(request, conn)
        try:
            try:
                response: Response = await as_first_completed(
                    [asyncio.wait_for(self._resp_future_dict[resp_future_id], timeout)],
                    not_cancel_future_list=[conn.conn_future],
                )
                response.state = request.state
                return response
            except asyncio.TimeoutError:
                raise asyncio.TimeoutError(f"msg_id:{resp_future_id} request timeout")
        finally:
            if resp_future_id in self._resp_future_dict:
                del self._resp_future_dict[resp_future_id]

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
            raise RPCError(f"request num must:{Constant.MSG_RESPONSE} not {response.msg_type}")
        if "exc" in response.body:
            exc_info: str = response.body.get("exc_info", "")
            if response.header.get("user_agent") == Constant.USER_AGENT:
                raise_rap_error(response.body["exc"], exc_info)
            else:
                raise rap_exc.RpcRunTimeError(exc_info)
        return response

    def channel(self, func_name: str, conn: Connection, group: Optional[str] = None) -> "Channel":
        """create and init channel
        func_name: rpc func name
        conn: channel transport conn
        group: func's group
        """

        async def create(_channel_id: str) -> None:
            """create recv queue"""
            self._channel_queue_dict[f"{conn.sock_tuple}:{_channel_id}"] = asyncio.Queue()

        async def read(_channel_id: str) -> Response:
            """read response|exc from queue"""
            result: Union[Response, Exception] = await self._channel_queue_dict[
                f"{conn.sock_tuple}:{_channel_id}"
            ].get()
            if isinstance(result, Exception):
                raise result
            return result

        async def write(request: Request) -> None:
            """write request to conn"""
            await self.write_to_conn(request, conn)

        async def close(_channel_id: str) -> None:
            """clear channel queue"""
            del self._channel_queue_dict[f"{conn.sock_tuple}:{_channel_id}"]

        target: str = f"/{group or Constant.DEFAULT_GROUP}/{func_name}"
        return Channel(self, target, conn, create, read, write, close)  # type: ignore

    #############
    # processor #
    #############
    def load_processor(self, processor_list: List[BaseProcessor]) -> None:
        """load client processor"""
        for processor in processor_list:
            self._process_request_list.append(processor.process_request)
            self._process_response_list.append(processor.process_response)
            self._process_exception_list.append(processor.process_exc)
