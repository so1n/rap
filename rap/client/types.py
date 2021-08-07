from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    from rap.client.core import BaseClient

CLIENT_EVENT_FN = Callable[["BaseClient"], Any]
