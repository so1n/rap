import importlib
import inspect
import logging
import os
from collections import OrderedDict
from dataclasses import dataclass, field
from types import FunctionType
from typing import Any, Callable, Dict, List, Optional, Type, Union

from rap.common.channel import BaseChannel
from rap.common.exceptions import RegisteredError
from rap.common.types import is_json_type


@dataclass()
class FuncModel(object):
    group: str
    type_: str
    func: Callable

    is_private: bool
    doc: Optional[str] = None
    name: Optional[str] = None
    arg_list: List[str] = field(default_factory=list)
    kwarg_dict: OrderedDict = field(default_factory=OrderedDict)
    return_type: Optional[Type] = None

    def __post_init__(self) -> None:
        if not self.doc:
            self.doc = self.func.__doc__
        if not self.name:
            self.name = self.func.__name__

        func_sig = inspect.signature(self.func)
        self.return_type = func_sig.return_annotation
        for name, parameter in func_sig.parameters.items():
            if parameter.default is parameter.empty:
                self.arg_list.append(name)
            else:
                self.kwarg_dict[name] = parameter.default


class RegistryManager(object):
    def __init__(self) -> None:
        self._cwd: str = os.getcwd()
        self.func_dict: Dict[str, FuncModel] = dict()

        self.register(self._load, "load", group="registry", is_private=True)
        self.register(self._reload, "reload", group="registry", is_private=True)
        self.register(self._get_register_func, "list", group="registry", is_private=True)

    @classmethod
    def gen_key(cls, group: str, name: str, type_: str) -> str:
        return f"{type_}:{group}:{name}"

    @staticmethod
    def _get_func_type(func: Callable) -> str:
        sig: "inspect.Signature" = inspect.signature(func)
        func_arg_parameter: List[inspect.Parameter] = [i for i in sig.parameters.values() if i.default == i.empty]

        func_type: str = "normal"
        try:
            if len(func_arg_parameter) == 1 and issubclass(func_arg_parameter[0].annotation, BaseChannel):
                func_type = "channel"
        except TypeError:
            # ignore error TypeError: issubclass() arg 1 must be a class
            pass
        return func_type

    def register(
        self,
        func: Callable,
        name: Optional[str] = None,
        group: str = "default",
        is_private: bool = False,
        doc: Optional[str] = None,
    ) -> None:
        """
        func: Function that need to be registered
        name: If the function name is not specified, the system will obtain its own name according to the function,
              otherwise it will be replaced by the specified function name
        group: Specify the group to which the function to be registered belongs.
               The same function can be registered to different groups.
               The root group is generally used for system components, and there are restrictions when calling.
        is_private: if True, it can only be accessed through the local cli
        """
        if inspect.isfunction(func) or inspect.ismethod(func):
            name = name if name else func.__name__
        else:
            raise RegisteredError("func must be func or method")

        sig: "inspect.Signature" = inspect.signature(func)

        type_: str = self._get_func_type(func)

        if type_ == "normal":
            # check func param&return value type hint
            if sig.return_annotation is sig.empty:
                raise RegisteredError(f"{func.__name__} must use TypeHints")
            if not is_json_type(sig.return_annotation):
                raise RegisteredError(f"{func.__name__} return type:{sig.return_annotation} is not json type")
            for param in sig.parameters.values():
                if param.annotation is sig.empty:
                    raise RegisteredError(f"{func.__name__} param:{param.name} must use TypeHints")
                if not is_json_type(param.annotation):
                    raise RegisteredError(
                        f"{func.__name__} param:{param.name} type:{param.annotation} is not json type"
                    )

        func_key: str = self.gen_key(group, name, type_)
        if func_key in self.func_dict:
            raise RegisteredError(f"Name: {name} has already been used")
        self.func_dict[func_key] = FuncModel(
            group=group, type_=type_, name=name, func=func, is_private=is_private, doc=doc
        )

        # not display log before called logging.basicConfig
        if not is_private:
            logging.info(f"register func:{func_key}")

    @staticmethod
    def _load_func(path: str, func_str: str) -> FunctionType:
        reload_module = importlib.import_module(path)
        func = getattr(reload_module, func_str)
        if not hasattr(func, "__call__"):
            raise RegisteredError(f"{func_str} is not a callable object")
        return func

    def _load(
        self,
        path: str,
        func_str: str,
        name: Optional[str] = None,
        group: str = "default",
        is_private: bool = False,
        doc: Optional[str] = None,
    ) -> str:
        """load func to registry"""
        try:
            func = self._load_func(path, func_str)
            if not name:
                name = func.__name__

            type_: str = self._get_func_type(func)
            func_key: str = self.gen_key(group, name, type_)
            if func_key in self.func_dict:
                raise RegisteredError(f"{name} already exists in group {group}")

            self.register(func, name, group, is_private, doc)
            return f"load {func_str} from {path} success"
        except Exception as e:
            raise RegisteredError(f"load {func_str} from {path} fail, {str(e)}")

    def _reload(
        self, path: str, func_str: str, name: Optional[str] = None, group: str = "default", doc: Optional[str] = None
    ) -> str:
        """reload func by registry"""
        try:
            func = self._load_func(path, func_str)
            if not name:
                name = func.__name__
            type_: str = self._get_func_type(func)
            func_key: str = self.gen_key(group, name, type_)
            if func_key not in self.func_dict:
                raise RegisteredError(f"{name} not in group {group}")

            func_model: FuncModel = self.func_dict[func_key]
            if func_model.is_private:
                raise RegisteredError(f"{func_key} reload fail, private func can not reload")
            self.func_dict[func_key] = FuncModel(
                group=group, type_=type_, name=name, func=func, is_private=func_model.is_private, doc=doc
            )
            return f"reload {func_str} from {path} success"
        except Exception as e:
            raise RegisteredError(f"reload {func_str} from {path} fail, {str(e)}")

    def _get_register_func(self) -> List[Dict[str, Union[str, bool]]]:
        """get func info which in registry"""
        register_list: List[Dict[str, Union[str, bool]]] = []
        for key, value in self.func_dict.items():
            module = inspect.getmodule(value.func)
            if not module:
                continue
            register_list.append(
                {
                    "key": key,
                    "name": value.name if value.name else "",
                    "module_name": module.__name__,
                    "module_file": module.__file__,
                    "doc": value.doc if value.doc else "",
                    "is_private": value.is_private if value.is_private else False,
                    "group": value.group,
                    "type": value.type_,
                }
            )
        return register_list

    def __contains__(self, key: str) -> bool:
        return key in self.func_dict

    def __getitem__(self, key: str) -> FuncModel:
        return self.func_dict[key]
