import importlib
import inspect
import logging
import os
from collections import OrderedDict
from types import FunctionType
from typing import Any, Callable, Dict, List, Optional, Type, Union

from rap.common.channel import get_corresponding_channel_class
from rap.common.exceptions import RegisteredError
from rap.common.types import is_json_type
from rap.common.utils import constant

logger: logging.Logger = logging.getLogger(__name__)


class FuncModel(object):
    def __init__(
        self,
        group: str,
        func_type: str,
        func: Callable,
        is_private: bool,
        doc: Optional[str] = None,
        func_name: Optional[str] = None,
    ) -> None:
        self.func_sig = inspect.signature(func)
        self.group: str = group
        self.func_type: str = func_type
        self.func: Callable = func
        self.is_gen_func: bool = inspect.isgenerator(func) or inspect.isasyncgenfunction(func)
        self.is_private: bool = is_private
        self.doc: str = doc or func.__doc__ or ""
        self.func_name: str = func_name or func.__name__
        self.return_type: Type = self.func_sig.return_annotation
        self.arg_list: List[str] = []
        self.kwarg_dict: OrderedDict = OrderedDict()

        if self.func_type == constant.CHANNEL_TYPE and self.is_gen_func:
            raise RegisteredError("Is not a legal function. is channel or gen func?")

        for name, parameter in self.func_sig.parameters.items():
            if parameter.default is parameter.empty:
                self.arg_list.append(name)
            else:
                self.kwarg_dict[name] = parameter.default

    def to_dict(self) -> Dict[str, Any]:
        return {
            "group": self.group,
            "func_type": self.func_type,
            "is_gen_func": self.is_gen_func,
            "is_private": self.is_private,
            "doc": self.doc,
            "func_name": self.func_name,
        }


class RegistryManager(object):
    """server func manager"""

    def __init__(self) -> None:
        self._cwd: str = os.getcwd()
        self.func_dict: Dict[str, FuncModel] = dict()

        self.register(self._load, "load", group="registry", is_private=True)
        self.register(self._reload, "reload", group="registry", is_private=True)
        self.register(self.get_register_func_list, "list", group="registry", is_private=True)

    @staticmethod
    def gen_key(group: str, name: str, type_: str) -> str:
        """gen func key"""
        return f"{type_}:{group}:{name}"

    @staticmethod
    def _get_func_type(func: Callable) -> str:
        """get func type, normal or channel"""
        func_type: str = constant.NORMAL_TYPE
        try:
            get_corresponding_channel_class(func)
            func_type = constant.CHANNEL_TYPE
        except TypeError:
            # ignore error TypeError: issubclass() arg 1 must be a class
            pass
        return func_type

    def register(
        self,
        func: Callable,
        name: Optional[str] = None,
        group: Optional[str] = None,
        is_private: bool = False,
        doc: Optional[str] = None,
    ) -> None:
        """
        register func to manager
        :param func: Function that need to be registered
        :param name: If the function name is not specified,
              the system will obtain its own name according to the function,
              otherwise it will be replaced by the specified function name
        :param group: Specify the correlation_id to which the function to be registered belongs.
               The same function can be registered to different groups.
               The root correlation_id is generally used for system components, and there are restrictions when calling.
        :param is_private: If the function is private, it will be restricted to call and cannot be overloaded
        :param doc: func doc, if not set, auto use python func doc
        """
        if inspect.isfunction(func) or inspect.ismethod(func):
            name = name if name else func.__name__
        else:
            raise RegisteredError("func must be func or method")

        sig: "inspect.Signature" = inspect.signature(func)

        func_type: str = self._get_func_type(func)

        if func_type == constant.NORMAL_TYPE:
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

        if group is None:
            group = constant.DEFAULT_GROUP

        func_key: str = self.gen_key(group, name, func_type)
        if func_key in self.func_dict:
            raise RegisteredError(f"`{func_key}` Already register")
        self.func_dict[func_key] = FuncModel(
            group=group, func_type=func_type, func_name=name, func=func, is_private=is_private, doc=doc
        )
        logger.debug(f"register `{func_key}` success")

    @staticmethod
    def _load_func(path: str, func_str: str) -> FunctionType:
        """Dynamic loading function
        :param path: func file path
        :param func_str: func name
        """
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
        group: Optional[str] = None,
        is_private: bool = False,
        doc: Optional[str] = None,
    ) -> str:
        """load func to registry"""
        try:
            func = self._load_func(path, func_str)
            if not name:
                name = func.__name__
            if group is None:
                group = constant.DEFAULT_GROUP

            func_type: str = self._get_func_type(func)
            func_key: str = self.gen_key(group, name, func_type)
            if func_key in self.func_dict:
                raise RegisteredError(f"`{func_key}` already exists")

            self.register(func, name, group, is_private, doc)
            return f"load {func_str} from {path} success"
        except Exception as e:
            raise RegisteredError(f"load {func_str} from {path} fail, {str(e)}")

    def _reload(
        self,
        path: str,
        func_str: str,
        name: Optional[str] = None,
        group: Optional[str] = None,
        doc: Optional[str] = None,
    ) -> str:
        """reload func by registry"""
        try:
            func = self._load_func(path, func_str)
            if not name:
                name = func.__name__
            if group is None:
                group = constant.DEFAULT_GROUP
            func_type: str = self._get_func_type(func)
            func_key: str = self.gen_key(group, name, func_type)
            if func_key not in self.func_dict:
                raise RegisteredError(f"`{func_key}` not exists")

            func_model: FuncModel = self.func_dict[func_key]
            if func_model.is_private:
                raise RegisteredError(f"{func_key} reload fail, private func can not reload")
            self.func_dict[func_key] = FuncModel(
                group=group, func_type=func_type, func_name=name, func=func, is_private=func_model.is_private, doc=doc
            )
            return f"reload {func_str} from {path} success"
        except Exception as e:
            raise RegisteredError(f"reload {func_str} from {path} fail, {str(e)}")

    def get_register_func_list(self) -> List[Dict[str, Union[str, bool]]]:
        """get func info which in registry"""
        register_list: List[Dict[str, Union[str, bool]]] = []
        for key, value in self.func_dict.items():
            module = inspect.getmodule(value.func)
            if not module:
                continue
            func_info_dict: Dict[str, Any] = value.to_dict()
            func_info_dict.update({"module_name": module.__name__, "module_file": module.__file__})
            register_list.append(func_info_dict)
        return register_list

    def __contains__(self, key: str) -> bool:
        return key in self.func_dict

    def __getitem__(self, key: str) -> FuncModel:
        return self.func_dict[key]
