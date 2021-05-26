from rap.common.exceptions import FuncNotFoundError
from rap.common.utils import Constant
from rap.server.model import Request
from rap.server.plugin.processor.base import BaseProcessor
from rap.server.registry import FuncModel


class PermissionProcessor(BaseProcessor):
    """
    According to `request`, you can know what the function of this request is.
     If it is a private function, it is limited to only localhost can execute
    """

    async def process_request(self, request: Request) -> Request:
        func_model: FuncModel = self.app.registry.get_func_model(
            request, Constant.NORMAL_TYPE if request.num == Constant.MSG_REQUEST else Constant.CHANNEL_TYPE
        )

        if func_model.is_private and request.header["host"][0] not in ("::1", "127.0.0.1", "localhost"):
            raise FuncNotFoundError(f"No permission to call:`{request.func_name}`")
        return request
