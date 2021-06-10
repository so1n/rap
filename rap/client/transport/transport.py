import asyncio
import logging
import random
import uuid
from typing import Any, Dict, List, Optional, Sequence, Tuple, Type, Union

from rap.client.model import Request, Response
from rap.client.processor.base import BaseProcessor
from rap.client.transport.channel import Channel
from rap.client.utils import get_exc_status_code_dict, raise_rap_error
from rap.common import exceptions as rap_exc
from rap.common.conn import Connection
from rap.common.exceptions import ChannelError, RPCError
from rap.common.types import BASE_REQUEST_TYPE, BASE_RESPONSE_TYPE
from rap.common.utils import Constant, Event, as_first_completed

__all__ = ["Transport"]


class Transport(object):
    """base client transport, encapsulation of custom transport protocol"""

    def __init__(
        self,
        read_timeout: int = 9,
        keep_alive_time: int = 1200,
    ):
        self._read_timeout: int = read_timeout
        self._keep_alive_time: int = keep_alive_time
        self._process_request_list: List = []
        self._process_response_list: List = []
        self._process_exception_list: List = []

        self._msg_id: int = random.randrange(65535)
        self._exc_status_code_dict = get_exc_status_code_dict()
        self._resp_future_dict: Dict[str, asyncio.Future[Response]] = {}
        self._channel_queue_dict: Dict[str, asyncio.Queue[Union[Response, Exception]]] = {}

    async def listen(self, conn: Connection) -> None:
        """listen server msg"""
        logging.debug(f"listen:%s start", conn.peer_tuple)
        try:
            while not conn.is_closed():
                await self._dispatch_resp_from_conn(conn)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            conn.set_reader_exc(e)
            logging.exception(f"listen {conn} error:{e}")
            if not conn.is_closed():
                await conn.await_close()

    async def _dispatch_resp_from_conn(self, conn: Connection) -> None:
        response, exc = await self._read_from_conn(conn)
        if not response:
            logging.error(str(exc))
            return
        if exc:
            raise exc

        resp_future_id: str = f"{conn.sock_tuple}:{response.msg_id}"
        channel_id: Optional[str] = response.header.get("channel_id")
        status_code: int = response.header.get("status_code", 500)

        async def put_exc_to_receiver(put_response: Response, put_exc: Exception) -> None:
            for process_exc in self._process_exception_list:
                put_response, put_exc = await process_exc(put_response, put_exc)
            if channel_id in self._channel_queue_dict:
                self._channel_queue_dict[channel_id].put_nowait(put_exc)
            elif put_response.msg_id != -1 and resp_future_id in self._resp_future_dict:
                self._resp_future_dict[resp_future_id].set_exception(put_exc)
            elif isinstance(put_exc, rap_exc.ServerError):
                raise put_exc
            else:
                logging.error(f"recv error msg:{response}, ignore")

        if exc:
            await put_exc_to_receiver(response, exc)
            return
        elif response.num == Constant.SERVER_ERROR_RESPONSE or status_code in self._exc_status_code_dict:
            # server error response handle
            exc_class: Type["rap_exc.BaseRapError"] = self._exc_status_code_dict.get(status_code, rap_exc.BaseRapError)
            await put_exc_to_receiver(response, exc_class(response.body))
            return
        elif response.num == Constant.SERVER_EVENT:
            # server event msg handle
            if response.func_name == Constant.EVENT_CLOSE_CONN:
                event_exc: Exception = ConnectionError(f"recv close conn event, event info:{response.body}")
                raise event_exc
            elif response.func_name == Constant.PING_EVENT:
                request: Request = Request.from_event(Event(Constant.PONG_EVENT, ""))
                await self.write_to_conn(request, conn)
                return
            elif response.func_name == Constant.DECLARE:
                if not response.body.get("result"):
                    raise rap_exc.AuthError("Declare error")
            else:
                logging.error(f"recv error event {response}")
        elif response.num == Constant.CHANNEL_RESPONSE and channel_id:
            # put msg to channel
            if channel_id not in self._channel_queue_dict:
                logging.error(f"recv {channel_id} msg, but {channel_id} not create")
            else:
                self._channel_queue_dict[channel_id].put_nowait(response)
            return
        elif response.num == Constant.MSG_RESPONSE and resp_future_id in self._resp_future_dict:
            # set msg to future_dict's `future`
            self._resp_future_dict[resp_future_id].set_result(response)
        else:
            logging.error(f"Can' parse response: {response}, ignore")
            return

    async def read_from_conn(self, conn: Connection) -> Optional[Response]:
        if conn.conn_id:
            raise rap_exc.RPCError("conn already declare, can not call read_from_conn")
        response, exc = await self._read_from_conn(conn)
        if exc:
            raise exc
        return response

    async def _read_from_conn(self, conn: Connection) -> Tuple[Optional[Response], Optional[Exception]]:
        """recv server msg handle"""
        try:
            response_msg: Optional[BASE_RESPONSE_TYPE] = await conn.read(self._keep_alive_time)
            logging.debug(f"recv raw data: %s", response_msg)
        except asyncio.TimeoutError as e:
            logging.error(f"recv response from {conn.connection_info} timeout")
            raise e

        if response_msg is None:
            raise ConnectionError("Connection has been closed")

        # parse response
        try:
            response: Response = Response.from_msg(response_msg)
        except Exception as e:
            e_msg: str = f"recv wrong response:{response_msg}, ignore error:{e}"
            return None, rap_exc.ProtocolError(e_msg)

        exc: Optional[Exception] = None
        try:
            for process_response in self._process_response_list:
                response = await process_response(response)
        except Exception as e:
            exc = e
        return response, exc

    async def declare(self, server_name: str, conn: Connection) -> str:
        request: Request = Request.from_event(Event(Constant.DECLARE, {"server_name": server_name}))
        await self.write_to_conn(request, conn)
        response: Optional[Response] = await self.read_from_conn(conn)

        exc: Exception = ConnectionError("create conn error")
        if not response:
            raise exc
        if response.num != Constant.SERVER_EVENT:
            raise exc
        if response.func_name == Constant.EVENT_CLOSE_CONN:
            raise ConnectionError(f"recv close conn event, event info:{response.body}")
        elif response.func_name == Constant.DECLARE:
            if response.body.get("result", False) and "conn_id" in response.body:
                return response.body["conn_id"]
        raise exc

    ####################################
    # base one by one request response #
    ####################################
    async def _base_request(self, request: Request, conn_list: List[Connection]) -> Response:
        """gen msg id, send and recv response"""
        msg_id: int = self._msg_id + 1
        request.msg_id = msg_id
        # Avoid too big numbers
        self._msg_id = msg_id & 65535

        exc: Exception = rap_exc.RPCError("request error")
        for conn in conn_list:
            try:
                resp_future_id: str = f"{conn.sock_tuple}:{request.msg_id}"
                self._resp_future_dict[resp_future_id] = asyncio.Future()
                await self.write_to_conn(request, conn)
                try:
                    try:
                        return await as_first_completed(
                            [asyncio.wait_for(self._resp_future_dict[resp_future_id], self._read_timeout)],
                            not_cancel_future_list=[conn.conn_future],
                        )
                    except asyncio.TimeoutError:
                        raise asyncio.TimeoutError(f"msg_id:{resp_future_id} request timeout")
                finally:
                    if resp_future_id in self._resp_future_dict:
                        del self._resp_future_dict[resp_future_id]
            except Exception as e:
                exc = e
        raise exc

    @staticmethod
    def before_write_handle(request: Request) -> None:
        """check and header"""

        def set_header_value(header_key: str, header_value: Any, is_cover: bool = False) -> None:
            """if key not in header, set header value"""
            if is_cover or header_key not in request.header:
                request.header[header_key] = header_value

        set_header_value("version", Constant.VERSION, is_cover=True)
        set_header_value("user_agent", Constant.USER_AGENT, is_cover=True)
        set_header_value("request_id", str(uuid.uuid4()), is_cover=True)

    #######################
    # base write_to_conn&read api #
    #######################
    async def write_to_conn(self, request: Request, conn: Connection) -> None:
        """write_to_conn msg to conn"""
        request.header["host"] = conn.peer_tuple

        async def _write(_request: Request) -> None:
            self.before_write_handle(_request)
            request.header["conn_id"] = conn.conn_id

            for process_request in self._process_request_list:
                _request = await process_request(_request)

            request_msg: BASE_REQUEST_TYPE = _request.to_msg()
            try:
                await conn.write(request_msg)  # type: ignore
            except asyncio.TimeoutError:
                raise asyncio.TimeoutError(
                    f"send to %s timeout, drop data:%s", conn.connection_info, request_msg  # type: ignore
                )
            except Exception as e:
                raise e

        if request.num == Constant.CHANNEL_REQUEST:
            if "channel_id" not in request.header:
                raise ChannelError("not found channel id in header")
            await _write(request)
        else:
            await _write(request)

    ######################
    # one by one request #
    ######################
    async def request(
        self,
        func_name: str,
        conn_list: List[Connection],
        arg_param: Optional[Sequence[Any]] = None,
        kwarg_param: Optional[Dict[str, Any]] = None,
        call_id: Optional[int] = None,
        group: Optional[str] = None,
        header: Optional[dict] = None,
    ) -> Response:
        """msg request handle"""
        group = group or Constant.DEFAULT_GROUP
        call_id = call_id or -1
        arg_param = arg_param or []
        kwarg_param = kwarg_param or {}
        request: Request = Request(
            Constant.MSG_REQUEST,
            func_name,
            {"call_id": call_id, "param": arg_param, "default_param": kwarg_param},
            group=group,
        )
        if header:
            request.header.update(header)
        response: Response = await self._base_request(request, conn_list)
        if response.num != Constant.MSG_RESPONSE:
            raise RPCError(f"request num must:{Constant.MSG_RESPONSE} not {response.num}")
        if "exc" in response.body:
            exc_info: str = response.body.get("exc_info", "")
            if response.header.get("user_agent") == Constant.USER_AGENT:
                raise_rap_error(response.body["exc"], exc_info)
            else:
                raise RuntimeError(exc_info)
        return response

    def channel(self, func_name: str, conn: Connection, group: Optional[str] = None) -> "Channel":
        async def create(_channel_id: str) -> None:
            self._channel_queue_dict[_channel_id] = asyncio.Queue()

        async def read(_channel_id: str) -> Response:
            result: Union[Response, Exception] = await self._channel_queue_dict[_channel_id].get()
            if isinstance(result, Exception):
                raise result
            return result

        async def write(request: Request) -> None:
            await self.write_to_conn(request, conn)

        async def close(_call_id: str) -> None:
            del self._channel_queue_dict[_call_id]

        return Channel(func_name, conn, create, read, write, close, group=group)

    #############
    # processor #
    #############
    def load_processor(self, processor_list: List[BaseProcessor]) -> None:
        for processor in processor_list:
            self._process_request_list.append(processor.process_request)
            self._process_response_list.append(processor.process_response)
            self._process_exception_list.append(processor.process_exc)
