from typing import Any, Dict, Optional, Type, TypeVar

from rap.common.utlis import Constant


class AsyncRedis(object):
    is_install_redis: bool = False

    def __init__(self, url: str, db: int = 0, namespace: Optional[str] = None, **kwargs: Dict[str, Any]):
        self.enable_redis: bool = False
        self._namespace: str = namespace if namespace else f"rap:{Constant.USER_AGENT}:{Constant.VERSION}:"
        self.client = None


try:
    from aredis import StrictRedis, StrictRedisCluster

    _T = TypeVar("_T", StrictRedis, StrictRedisCluster)

    class AsyncRedis(object):
        is_install_redis: bool = True

        def __init__(self, url: str, db: int = 0, namespace: Optional[str] = None, **kwargs: Dict[str, Any]):
            self._namespace: str = namespace if namespace else f"rap:{Constant.USER_AGENT}:{Constant.VERSION}:"
            if "model" in kwargs:
                model: Type[_T] = kwargs.pop("model")
            else:
                model = StrictRedis
            self.client: _T = model.from_url(url, db, **kwargs)
            self.enable_redis: bool = True


except ImportError:
    pass
