import importlib
import inspect
import logging
import os
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Tuple

from rap.common.channel import BaseChannel
from rap.common.exceptions import RegisteredError
from rap.common.types import check_is_json_type


@dataclass()
class FuncModel(object):
    group: str
    type_: str
    name: str
    func: Callable


class FuncManager(object):
    def __init__(self):
        self._cwd: str = os.getcwd()
        self.func_dict: Dict[str, FuncModel] = dict()

        self.register(self._load, "load", group="root")
        self.register(self._reload, "reload", group="root")
        self.register(self._get_register_func, "list", group="root")

    @staticmethod
    def _get_func_type(func: Callable) -> str:
        sig: "inspect.Signature" = inspect.signature(func)
        func_arg_parameter: List[inspect.Parameter] = [i for i in sig.parameters.values() if i.default == i.empty]

        func_type: str = "normal"
        if len(func_arg_parameter) == 1 and issubclass(func_arg_parameter[0].annotation, BaseChannel):
            func_type = "channel"
        return func_type

    def register(self, func: Optional[Callable], name: Optional[str] = None, group: str = "default"):
        """
        func: Function that need to be registered
        name: If the function name is not specified, the system will obtain its own name according to the function,
              otherwise it will be replaced by the specified function name
        group: Specify the group to which the function to be registered belongs.
               The same function can be registered to different groups.
               The root group is generally used for system components, and there are restrictions when calling.
        """
        sig: "inspect.Signature" = inspect.signature(func)

        type_: str = self._get_func_type(func)

        if type_ == "normal":
            # check func param&return value type hint
            if not check_is_json_type(sig.return_annotation) or sig.return_annotation is sig.empty:
                raise RegisteredError(f"{func.__name__} return type:{sig.return_annotation} is not json type")
            for param in sig.parameters.values():
                if not check_is_json_type(param.annotation) or param.annotation is sig.empty:
                    raise RegisteredError(
                        f"{func.__name__} param:{param.name} type:{param.annotation} is not json type"
                    )

        if not hasattr(func, "__call__"):
            raise RegisteredError(f"{name} is not a callable object")
        # func name handle
        if inspect.isfunction(func) or inspect.ismethod(func):
            name: str = name if name else func.__name__
        else:
            raise RegisteredError("func must be func or method")

        func_key: str = f"{group}:{type_}:{name}"
        if func_key in self.func_dict:
            raise RegisteredError(f"Name: {name} has already been used")
        self.func_dict[func_key] = FuncModel(group=group, type_=type_, name=name, func=func)

        # not display log before called logging.basicConfig
        if group != "root":
            logging.info(f"register func:{func_key}")

    @staticmethod
    def _load_func(path: str, func_str: str) -> Callable:
        reload_module = importlib.import_module(path)
        func = getattr(reload_module, func_str)
        if not hasattr(func, "__call__"):
            raise RegisteredError(f"{func_str} is not a callable object")
        return func

    def _load(self, path: str, func_str: str, name: Optional[str] = None, group: str = "default") -> str:
        try:
            if group == "root":
                raise RegisteredError("Can't load root func")

            func = self._load_func(path, func_str)
            self.register(func, name, group)
            return f"load {func_str} from {path} success"
        except Exception as e:
            raise RegisteredError(f"load {func_str} from {path} fail, {str(e)}")

    def _reload(self, path: str, func_str: str, name: Optional[str] = None, group: str = "default") -> str:
        try:
            if group == "root":
                raise RegisteredError("Can't load root func")

            func = self._load_func(path, func_str)
            type_: str = self._get_func_type(func)
            func_key: str = f"{group}:{type_}:{name}"
            if func_key not in self.func_dict:
                raise RegisteredError(f"Name: {name} not in register table")
            self.func_dict[func_key] = FuncModel(group=group, type_=type_, name=name, func=func)
            return f"reload {func_str} from {path} success"
        except Exception as e:
            raise RegisteredError(f"reload {func_str} from {path} fail, {str(e)}")

    def _get_register_func(self) -> List[Tuple[str, str, str]]:
        register_list: List[Tuple[str, str, str]] = []
        for key, value in self.func_dict.items():
            module = inspect.getmodule(value.func)
            module_name: str = module.__name__
            module_file: str = module.__file__
            register_list.append((key, module_name, module_file))
        return register_list

    def __contains__(self, key: str) -> bool:
        return key in self.func_dict

    def __getitem__(self, key: str) -> FuncModel:
        return self.func_dict[key]


func_manager: "FuncManager" = FuncManager()
