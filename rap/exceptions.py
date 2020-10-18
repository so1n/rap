class BaseRapError(Exception):
    pass


class AuthError(BaseRapError):
    def __init__(self, message: str = 'Auth Error'):
        super().__init__(message)


class FuncNotFoundError(BaseRapError):
    pass


class LifeStateError(BaseRapError):
    def __init__(self, message: str = 'life state error'):
        super().__init__(message)


class ProtocolError(BaseRapError):
    def __init__(self, message: str = 'Invalid protocol'):
        super().__init__(message)


class RegisteredError(BaseRapError):
    pass


class RPCError(BaseRapError):
    def __init__(self, message: str):
        super().__init__(message)


class RpcRunTimeError(BaseRapError):
    def __init__(self, parent, message):
        super().__init__(self, f"{parent}: {message}")


class ServerError(BaseRapError):
    def __init__(self, message: str = 'Server Error'):
        super().__init__(message)
