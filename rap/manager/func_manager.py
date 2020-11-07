import inspect
import importlib
import logging
import os

from typing import Callable, Dict, List, Optional, Tuple

from rap.common.exceptions import RegisteredError
from rap.common.types import check_is_json_type


class FuncManager(object):
    def __init__(self):
        self._cwd: str = os.getcwd()
        self.func_dict: Dict[str, Callable] = dict()

        self.register(self._load, "_root_load")
        self.register(self._reload, "_root_reload")
        self.register(self._get_register_func, "_root_list")

    def register(self, func: Optional[Callable], name: Optional[str] = None, is_root: bool = False):
        # check func param&return value type hint
        sig: "inspect.Signature" = inspect.signature(func)
        if not check_is_json_type(sig.return_annotation) or sig.return_annotation is sig.empty:
            raise RegisteredError(f"{func.__name__} return type:{sig.return_annotation} is not json type")
        for param in sig.parameters.values():
            if not check_is_json_type(param.annotation) or param.annotation is sig.empty:
                raise RegisteredError(f"{func.__name__} param:{param.name} type:{param.annotation} is not json type")

        # func name handle
        if inspect.isfunction(func) or inspect.ismethod(func):
            name: str = name if name else func.__name__
        else:
            raise RegisteredError("func must be func or method")
        if not hasattr(func, "__call__"):
            raise RegisteredError(f"{name} is not a callable object")
        if is_root and not name.startswith("_root_"):
            name = "_root_" + name
        if name in self.func_dict:
            raise RegisteredError(f"Name: {name} has already been used")
        self.func_dict[name] = func

        # not display log before called logging.basicConfig
        if not name.startswith("_root_"):
            logging.info(f"register func:{name}")

    def _load(self, path: str, func_str: str) -> str:
        try:
            reload_module = importlib.import_module(path)
            func = getattr(reload_module, func_str)
            self.register(func)
            return f"load {func_str} from {path} success"
        except Exception as e:
            raise RegisteredError(f"load {func_str} from {path} fail, {str(e)}")

    def _reload(self, path: str, func_str: str) -> str:
        try:
            if func_str not in self.func_dict:
                raise RegisteredError(f"func:{func_str} not found")
            reload_module = importlib.import_module(path)
            func = getattr(reload_module, func_str)
            if not hasattr(func, "__call__"):
                raise RegisteredError(f"{func_str} is not a callable object")
            self.func_dict[func_str] = func
            return f"reload {func_str} from {path} success"
        except Exception as e:
            raise RegisteredError(f"reload {func_str} from {path} fail, {str(e)}")

    def _get_register_func(self) -> List[Tuple[str, str, str]]:
        register_list: List[Tuple[str, str, str]] = []
        for key, value in self.func_dict.items():
            module = inspect.getmodule(value)
            module_name: str = module.__name__
            module_file: str = module.__file__
            register_list.append((key, module_name, module_file))
        return register_list


func_manager: "FuncManager" = FuncManager()
