from typing import Union, Tuple


class Event(object):
    def __init__(self, event_name: str, event_info: Union[str, dict]):
        self.event_name: str = event_name
        self.event_info: Union[str, dict] = event_info

    def to_tuple(self) -> Tuple[str, Union[str, dict]]:
        return self.event_name, self.event_info


class CloseConnEvent(Event):
    def __init__(self, event_info: Union[str, dict]):
        super().__init__("event_close_conn", event_info)


class PingEvent(Event):
    def __init__(self, event_info: Union[str, dict]):
        super().__init__("ping", event_info)


class PongEvent(Event):
    def __init__(self, event_info: Union[str, dict]):
        super().__init__("pong", event_info)


class DeclareEvent(Event):
    def __init__(self, event_info: Union[str, dict]):
        super().__init__("declare", event_info)


class DropEvent(Event):
    def __init__(self, event_info: Union[str, dict]):
        super().__init__("drop", event_info)
