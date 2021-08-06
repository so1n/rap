from typing import Any, Callable, TYPE_CHECKING

if TYPE_CHECKING:
    from rap.client.core import BaseClient

CLIENT_EVENT_FN = Callable[["BaseClient"], Any]