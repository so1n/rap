from typing import Tuple, Union


class Event(object):
    event_name: str

    def __init__(self, event_info: Union[str, dict], event_name: str = ""):
        if event_name:
            self.event_name = event_name
        self.event_info: Union[str, dict] = event_info

    def to_tuple(self) -> Tuple[str, Union[str, dict]]:
        return self.event_name, self.event_info


class CloseConnEvent(Event):
    event_name: str = "event_close_conn"

    def __init__(self, event_info: Union[str, dict]):
        super().__init__(event_info)


class PingEvent(Event):
    event_name: str = "ping"

    def __init__(self, event_info: Union[str, dict]):
        super().__init__(event_info)


class PongEvent(Event):
    event_name: str = "pong"

    def __init__(self, event_info: Union[str, dict]):
        super().__init__(event_info)


class DeclareEvent(Event):
    event_name: str = "declare"

    def __init__(self, event_info: Union[str, dict]):
        super().__init__(event_info)


class DropEvent(Event):
    event_name: str = "drop"

    def __init__(self, event_info: Union[str, dict]):
        super().__init__(event_info)


class ShutdownEvent(Event):
    event_name: str = "shutdown"

    def __init__(self, event_info: Union[str, dict]):
        super().__init__(event_info)
