import asyncio
import logging
import signal
from typing import Any, Callable, Dict, List, Optional

_signal_dict_list: Dict[int, List[Callable]] = {}
logger: logging.Logger = logging.getLogger(__name__)


def _signal_handler(signum: int, frame: Any) -> None:
    logger.debug("Receive signal %s, run shutdown...", signum)
    for callback in _signal_dict_list[signum]:
        try:
            callback(signum, frame)
        except Exception as e:
            logger.exception(f"Receive signal:{signum}, run callback:{callback} error:{e}")


def add_signal_handler(
    sig: int, callback: Callable[[int, Any], None], loop: Optional[asyncio.AbstractEventLoop] = None
) -> None:
    """Add signal callback"""
    if sig not in _signal_dict_list:
        try:
            # only use in unix
            if not loop:
                loop = asyncio.get_event_loop()
            loop.add_signal_handler(sig, _signal_handler, sig, None)
        except NotImplementedError:
            signal.signal(sig, _signal_handler)
        _signal_dict_list[sig] = []
    _signal_dict_list[sig].append(callback)


def remove_signal_handler(
    sig: int, callback: Callable[[int, Any], None], loop: Optional[asyncio.AbstractEventLoop] = None
) -> None:
    """remove signal callback"""
    assert sig in _signal_dict_list, f"{sig} not found"
    _signal_dict_list[sig].remove(callback)
    if not _signal_dict_list[sig]:
        try:
            # only use in unix
            if not loop:
                loop = asyncio.get_event_loop()
            loop.remove_signal_handler(sig)
        except NotImplementedError as e:
            logger.error(f"Not support remove signal:{e}")
