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
