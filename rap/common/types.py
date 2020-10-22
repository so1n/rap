import asyncio
import msgpack

from typing import Any, Tuple

LOOP_TYPE = asyncio.get_event_loop
READER_TYPE = asyncio.streams.StreamReader
WRITER_TYPE = asyncio.streams.StreamWriter
UNPACKER_TYPE = msgpack.Unpacker


BASE_REQUEST_TYPE = Tuple[int, int, dict, Any]
BASE_RESPONSE_TYPE = Tuple[int, int, dict, Any]
