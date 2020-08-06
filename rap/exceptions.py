class ProtocolError(Exception):
    def __init__(self, message: str = 'Invalid protocol'):
        super().__init__(message)


class FuncNotFoundError(Exception):
    pass


class RPCError(Exception):
    def __init__(self, message: str):
        super().__init__(message)


class ServerError(Exception):
    def __init__(self, message: str = 'Server Error'):
        super().__init__(message)


class RpcRunTimeError(Exception):
    def __init__(self, parent, message):
        self.parent = parent
        self.message = message
        Exception.__init__(self, "{0}: {1}".format(parent, message))


class RegisteredError(Exception):
    pass
