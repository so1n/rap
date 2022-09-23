from starlette.requests import Request
from starlette.responses import JSONResponse

from rap.api_gateway.exception import BaseError
from rap.common.exceptions import BaseRapError


async def api_exception(request: Request, exc: Exception) -> JSONResponse:
    if isinstance(exc, BaseRapError):
        return JSONResponse({"code": exc.status_code, "msg": exc.message})
    elif isinstance(exc, BaseError):
        return JSONResponse(exc.dict)
    return JSONResponse({"code": 1, "msg": str(exc)})
