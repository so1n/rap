"""
copy from:https://github.com/tarzanjw/pysnowflake/edit/master/snowflake/server/generator.py
"""
import logging
import os
import socket
import time
from typing import Dict, Tuple
from typing_extensions import TypedDict


EPOCH_TIMESTAMP: int = 550281600000
StatsTypedDict = TypedDict(
    "StatsTypedDict",
    {
        'dc': int,
        'worker': int,
        'timestamp': int,
        'last_timestamp': int,
        'sequence': int,
        'sequence_overload': int,
        'errors': int
    }
)


class Snowflake(object):
    def __init__(self, dc: int, worker: int):
        self.dc: int = dc
        self.worker: int = worker
        self.node_id: int = ((self.dc & 0x03) << 8) | (self.worker & 0xff)
        self.last_timestamp: int = EPOCH_TIMESTAMP
        self.sequence: int = 0
        self.sequence_overload: int = 0
        self.errors: int = 0
        self.generated_ids: int = 0

    def get_next_id(self) -> int:
        curr_time: int = int(time.time() * 1000)

        if curr_time < self.last_timestamp:
            # stop handling requests til we've caught back up
            self.errors += 1

        if curr_time > self.last_timestamp:
            self.sequence = 0
            self.last_timestamp = curr_time

        self.sequence += 1

        if self.sequence > 4095:
            # the sequence is overload, just wait to next sequence
            logging.warning('The sequence has been overload')
            self.sequence_overload += 1
            time.sleep(0.001)
            return self.get_next_id()

        generated_id: int = ((curr_time - EPOCH_TIMESTAMP) << 22) | (self.node_id << 12) | self.sequence

        self.generated_ids += 1
        return generated_id

    @property
    def stats(self) -> StatsTypedDict:
        return {
            'dc': self.dc,
            'worker': self.worker,
            'timestamp': int(time.time()*1000),  # current timestamp for this worker
            'last_timestamp': self.last_timestamp,  # the last timestamp that generated ID on
            'sequence': self.sequence,  # the sequence number for last timestamp
            'sequence_overload': self.sequence_overload,  # the number of times that the sequence is overflow
            'errors': self.errors,  # the number of times that clock went backward
        }


_cache: Dict[Tuple[int, int], Snowflake]  = {}


def get_snowflake_id() -> int:
    dc: int = hash(socket.gethostname())
    pid: int = os.getpid()
    dc_worker_key: Tuple[int, int] = (dc, pid)
    if dc_worker_key not in _cache:
        _cache[dc_worker_key] = Snowflake(dc, pid)
    return _cache[dc_worker_key].get_next_id()
