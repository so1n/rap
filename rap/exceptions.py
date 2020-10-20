from typing import Optional


class BaseRapError(Exception):
    message: str = 'Error'

    def __init__(self, message: Optional[str] = None):
        if message is None:
            message = self.message
        super().__init__(message)


class AuthError(BaseRapError):
    message: str = 'Auth Error'


class FuncNotFoundError(BaseRapError):
    message: str = 'Func not found'


class LifeCycleError(BaseRapError):
    message: str = 'Life cycle error'


class ParseError(BaseRapError):
    message: str = 'Parse error'


class ProtocolError(BaseRapError):
    message: str = 'Invalid protocol'


class RegisteredError(BaseRapError):
    message: str = 'Register Error'


class RPCError(BaseRapError):
    message: str = 'Rpc error'


class RpcRunTimeError(BaseRapError):
    message: str = 'Rpc run time error'


class ServerError(BaseRapError):
    message: str = 'Server error'
