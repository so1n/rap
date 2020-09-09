import asyncio
import msgpack

from typing import Any, Tuple, Union

LOOP_TYPE = asyncio.get_event_loop
READER_TYPE = asyncio.streams.StreamReader
WRITER_TYPE = asyncio.streams.StreamWriter
UNPACKER_TYPE = msgpack.Unpacker

# ('request num: 0', 'msg id', 'call id', 'is encrypt (0 false 1 true)', 'func name', 'param')
REQUEST_TYPE = Tuple[int, int, int, int, Union[bytes, str, None], Union[Tuple[Any, ...], bytes]]
# ('response num: 1', 'msg id', 'call id', 'is encrypt (0 false 1 true)', 'exc, exc info', 'result')
RESPONSE_TYPE = Tuple[int, int, int, int, Union[bytes, Tuple[str, str], None], Any]

# init(Verify encryption key and obtain client id)
#   client send msg
#   ('requests num: 10', 'msg id', 'encrypt key index', 'hello msg')
#   server send msg(success)
#   ('requests num: 11', 'msg id', 'is encrypt (0 false 1 true)', 'client id')
#   server send msg(error)
#   ('requests num: 11', 'msg id', 'is encrypt (0 false 1 true)', "''")
INIT_REQUEST_TYPE = Tuple[int, int, str, Union[str, bytes]]
INIT_RESPONSE_TYPE = Tuple[int, int, int, Union[str, bytes]]
# msg
#   client send msg
#   ('requests num: 20', 'msg id', 'call id', 'client id', '(func name, param)')
#   server send success msg
#   ('response num: 21', 'msg id', 'call id', 'client id', '("", result)')
#   server send error msg
#   ('response num: 21', 'msg id', 'call id', 'client id', '(exc, exc info)')
MSG_REQUEST_TYPE = Tuple[int, int, int, int, Union[Tuple[str, Any], bytes]]
MSG_RESPONSE_TYPE = Tuple[int, int, int, int, Union[Tuple[str, Any], bytes]]
# DROP(Clear the relevant information on the server and exit)
#   client send msg
#   ('requests num: 0', 'msg id', 'call id', 'client id', 'hello msg')
#   server send msg
#   ('requests num: 1', 'msg id', 'call id', 'client id', 'result 0 false 1 true')
DROP_REQUEST_TYPE = Tuple[int, int, int, int, Union[str]]
DROP_RESPONSE_TYPE = Tuple[int, int, int, int, Union[int]]

# param description
#   hello msg: When encrypt is enabled, the server verifies whether the same secret key is by decrypting hello msg
#   client id: hash(client ip) + random str


# 添加一个客户端与服务器的生命周期管理实例,目前有init, msg, drop,三个阶段, 同时管理client的一些资源
# 每次接受到消息时就更新timestamp. 初始化完成后会 会执行定时任务,每隔一段时间检查timestamp如果超时则自动销毁.
# 调用DROP时会自动销毁实例,客户端应该在断开前调用drop

