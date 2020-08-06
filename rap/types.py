import asyncio
import msgpack

from typing import Any, Tuple, Union

LOOP_TYPE = asyncio.get_event_loop
READER_TYPE = asyncio.streams.StreamReader
WRITER_TYPE = asyncio.streams.StreamWriter
UNPACKER_TYPE = msgpack.Unpacker

# ('request num: 0', 'msg id', 'call id', 'is encrypt (0 false 1 true)', 'func name', 'param')
REQUEST_TYPE = Tuple[int, int, int, int, str, Tuple[Any, ...]]
# ('response num: 1', '消息id', 'call id', 'is encrypt (0 false 1 true)', 'exc', 'result')
RESPONSE_TYPE = Tuple[int, int, int, int, Union[Tuple[str, str], None], Any]
