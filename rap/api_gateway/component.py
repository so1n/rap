from typing import Optional, Set

from starlette.requests import Request
from starlette.responses import JSONResponse
from typing_extensions import TypedDict

from rap.api_gateway.exception import BaseError
from rap.client import Client
from rap.common.exceptions import BaseRapError


async def api_exception(request: Request, exc: Exception) -> JSONResponse:
    if isinstance(exc, BaseRapError):
        return JSONResponse({"code": exc.status_code, "msg": exc.message})
    elif isinstance(exc, BaseError):
        return JSONResponse(exc.dict)
    return JSONResponse({"code": 1, "msg": str(exc)})


class RapClientTypedDict(TypedDict, total=False):
    title: str
    client: Client
    private_filter: Optional[bool]
    group_filter: Optional[Set[str]]
    func_info_dict: dict
