from typing import Tuple, Union

from rap.common.utils import constant


class Event(object):
    event_name: str

    def __init__(self, event_info: Union[str, dict], event_name: str = ""):
        if event_name:
            self.event_name = event_name
        self.event_info: Union[str, dict] = event_info

    def to_tuple(self) -> Tuple[str, Union[str, dict]]:
        return self.event_name, self.event_info


class _RapEvent(Event):
    def __init__(self, event_info: Union[str, dict]):
        super().__init__(event_info)


class CloseConnEvent(_RapEvent):
    event_name: str = constant.CLOSE_EVENT


class PingEvent(_RapEvent):
    event_name: str = constant.PING_EVENT


class DeclareEvent(_RapEvent):
    event_name: str = constant.DECLARE


class DropEvent(_RapEvent):
    event_name: str = constant.DROP


class ShutdownEvent(Event):
    event_name: str = "shutdown"

    def __init__(self, event_info: Union[str, dict]):
        super().__init__(event_info)
