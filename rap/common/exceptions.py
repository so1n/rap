from typing import Optional


class BaseRapError(Exception):
    status_code: int = 500
    message: str = "Error"

    def __init__(self, message: Optional[str] = None, extra_msg: Optional[str] = None):
        if message is None:
            message = self.message
        if extra_msg:
            message += f'. {extra_msg}'
        super().__init__(message)


class AuthError(BaseRapError):
    status_code: int = 501
    message: str = "Auth Error"


class FuncNotFoundError(BaseRapError):
    status_code: int = 502
    message: str = "Not found func"


class LifeCycleError(BaseRapError):
    status_code: int = 503
    message: str = "Life cycle error"


class ParseError(BaseRapError):
    status_code: int = 504
    message: str = "Parse error"


class ProtocolError(BaseRapError):
    status_code: int = 505
    message: str = "Invalid protocol"


class RegisteredError(BaseRapError):
    status_code: int = 506
    message: str = "Register Error"


class RPCError(BaseRapError):
    status_code: int = 507
    message: str = "Rpc error"


class RpcRunTimeError(BaseRapError):
    status_code: int = 508
    message: str = "Rpc run time error"


class ServerError(BaseRapError):
    status_code: int = 500
    message: str = "Server error"
