from starlette.requests import Request
from starlette.responses import JSONResponse

from rap.common.exceptions import BaseRapError


async def api_exception(request: Request, exc: Exception) -> JSONResponse:
    if isinstance(exc, BaseRapError):
        return JSONResponse({"code": exc.status_code, "msg": exc.message})
    return JSONResponse({"code": 1, "msg": str(exc)})
