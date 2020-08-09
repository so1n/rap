import inspect
import importlib
import logging
import os

from typing import Callable, Dict, Optional

from rap.exceptions import RegisteredError


class FuncManager(object):
    def __init__(self):
        self._cwd: str = os.getcwd()
        self.func_dict: Dict[str, Callable] = dict()
        self.generator_dict: dict = {}
        self.register(self.load, '_root_load')
        self.register(self.reload, '_root_reload')

    def register(self, func: Optional[Callable], name: Optional[str] = None):
        if inspect.isfunction(func) or inspect.ismethod(func):
            name: str = name if name else func.__name__
        else:
            raise RegisteredError("func must be func or method")
        if not hasattr(func, "__call__"):
            raise RegisteredError(f"{name} is not a callable object")
        if name in self.func_dict:
            raise RegisteredError(f"Name {name} has already been used")
        self.func_dict[name] = func
        logging.info(f"register func:{name}")

    def load(self, path: str, func_str: str) -> str:
        reload_module = importlib.import_module(path)
        func = getattr(reload_module, func_str)
        self.register(func)
        return f"load {func_str} from {path} success"

    def reload(self, path: str, func_str: str) -> str:
        if func_str not in self.func_dict:
            raise RegisteredError(f"func:{func_str} not found")
        reload_module = importlib.import_module(path)
        func = getattr(reload_module, func_str)
        if not hasattr(func, "__call__"):
            raise RegisteredError(f"{func_str} is not a callable object")
        self.func_dict[func_str] = func
        return f"reload {func_str} from {path} success"


func_manager = FuncManager()