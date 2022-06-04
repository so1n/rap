from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    from rap.server.core import Server

SERVER_EVENT_FN = Callable[["Server"], Any]


__all__ = ["SERVER_EVENT_FN", "Server"]
