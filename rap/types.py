import asyncio
import msgpack

from typing import Any, Dict, Tuple, Union

LOOP_TYPE = asyncio.get_event_loop
READER_TYPE = asyncio.streams.StreamReader
WRITER_TYPE = asyncio.streams.StreamWriter
UNPACKER_TYPE = msgpack.Unpacker

# ('request num: 0', 'msg id', 'call id', 'is encrypt (0 false 1 true)', 'func name', 'param')
REQUEST_TYPE = Tuple[int, int, int, int, Union[bytes, str, None], Union[Tuple[Any, ...], bytes]]
# ('response num: 1', 'msg id', 'call id', 'is encrypt (0 false 1 true)', 'exc, exc info', 'result')
RESPONSE_TYPE = Tuple[int, int, int, int, Union[bytes, Tuple[str, str], None], Any]


BASE_REQUEST_TYPE = Tuple[int, int, str, Union[Tuple, bytes]]
BASE_RESPONSE_TYPE = Tuple[int, int, Union[Tuple, bytes]]


# init(Verify encryption key and obtain client id)
#   client send msg
#   ('requests num: 10', 'msg id<int>', 'encrypt key<str>', {timestamp:<int>, nonce:<str>, client_version:<str>})
#   server send msg(success)
#   ('requests num: 11', 'msg id<int>', {timestamp:<int>, nonce:<str>, server_version:<str>, client_id:<str>})
INIT_REQUEST_TYPE = Tuple[int, int, str, Union[dict, bytes]]
INIT_RESPONSE_TYPE = Tuple[int, int, Union[dict, bytes]]
# msg
#   client send msg
#   ('requests num: 20', 'msg id<int>', 'client id<str>', {timestamp:<int>, nonce:<str>, client_version:<str>, call_id:<int>, func_name:<str>, param:<tuple>})
#   server send success msg
#   ('response num: 21', 'msg id<int>', {timestamp:<int>, nonce:<str>, server_version:<str>, call_id:<int>, func_name:<str>, result:<any>})
#   server send error msg
#   ('response num: 21', 'msg id<int>', {timestamp:<int>, nonce:<str>, server_version:<str>, call_id:<int>, func_name:<str>, exc:<str>, exc_info:<str>})
MSG_REQUEST_TYPE = Tuple[int, int, str, Union[dict, bytes]]
MSG_RESPONSE_TYPE = Tuple[int, int, Union[dict, bytes]]
# DROP(Clear the relevant information on the server and exit)
#   client send msg
#   ('requests num: 0', 'msg id', 'client id', {timestamp:<int>, nonce:<str>, client_version:<str>, call_id:<int>})
#   server send msg
#   ('requests num: 1', 'msg id', {timestamp:<int>, nonce:<str>, server_version:<str>, call_id:<int>, result<int>(1 true, 0 false)})
DROP_REQUEST_TYPE = Tuple[int, int, str, Union[Tuple[int, str], bytes]]
DROP_RESPONSE_TYPE = Tuple[int, int, Union[Tuple[int, int], bytes]]

# server push msg
# ('response num: 31, random msg id, '(event, ...)')
SERVER_PUSH_TYPE = Tuple[int, str, Union[Tuple, bytes]]
# server exception msg
# ('response num: 32, random msg id, 'exc, exc_info')
SERVER_ERROR_RESPONSE_TYPE = Tuple[int, str, Tuple[str, str]]

# param description
#   hello msg: When encrypt is enabled, the server verifies whether the same secret key is by decrypting hello msg
#   client id: hash(client ip) + random str
#   msg_id:
#       客户端发送消息会带上正数的msg id,每次会递增1,服务端会返回相同的msg id
#       如果服务端返回的msg id 为-1则代表服务端无法解析到客户端请求的msg id


# 添加一个客户端与服务器的生命周期管理实例,目前有init, msg, drop,三个阶段, 同时管理client的一些资源
# 每次接受到消息时就更新timestamp. 初始化完成后会 会执行定时任务,每隔一段时间检查timestamp如果超时则自动销毁.
# 调用DROP时会自动销毁实例,客户端应该在断开前调用drop

