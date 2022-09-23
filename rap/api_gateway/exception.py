from typing import Optional


class BaseError(Exception):
    code: int = 9999
    msg: str = "System Error"

    def __init__(self, code: Optional[int] = None, msg: Optional[str] = None, extra_msg: Optional[str] = None):
        self._code: int = code or self.code
        self._msg: str = msg or self.msg
        if extra_msg:
            self._msg = f"{self._msg}({extra_msg})"
        super().__init__(self.dict)

    @property
    def dict(self) -> dict:
        return {"code": self._code, "msg": self._msg}


class ParamError(BaseError):
    code: int = 1
    msg: str = "Param error"


class ServerNameError(BaseError):
    code: int = 2
    msg: str = "Server Name error"


class NotFoundError(BaseError):
    code: int = 3
    msg: str = "Not Found"
