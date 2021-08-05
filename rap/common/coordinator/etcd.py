import asyncio
import inspect
import json
import logging
from typing import Any, AsyncGenerator, Callable, List, Optional

from etcd3 import AioClient
from etcd3.models import EventEventType
from mypy_extensions import TypedDict

from .bass import BaseCoordinator

logger: logging.Logger = logging.getLogger()
ETCD_EVENT_VALUE_DICT_TYPE = TypedDict("ETCD_EVENT_VALUE_DICT_TYPE", {"key": str, "value": dict})


class EtcdClient(BaseCoordinator):
    """TODO replace etcd client"""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 2379,
        ttl: int = 60,
        namespace: str = "rap",
        cert_path: Optional[str] = None,
        key_path: Optional[str] = None,
        ca_path: Optional[str] = None,
    ) -> None:
        self.namespace: str = namespace
        if cert_path and key_path and ca_path:
            self._client: AioClient = AioClient(host, port, cert=(cert_path, key_path), verify=ca_path)
        else:
            self._client = AioClient(host, port)
        self._ttl: int = ttl

        self._lease_id: int = 0
        self._heartbeat_future: Optional[asyncio.Future] = None

    async def stop(self) -> None:
        if self._heartbeat_future and not self._heartbeat_future.done() and not self._heartbeat_future.cancelled():
            self._heartbeat_future.cancel()
        if self._lease_id:
            await self._client.lease_revoke(self._lease_id)
        await self._client.close()

    async def _heartbeat(self) -> None:
        try:
            while True:
                logger.debug(f"heartbeat by etcd, id: {self._lease_id}")
                try:
                    await self._client.lease_keep_alive(b'{"ID":%d}\n' % self._lease_id).resp
                    await asyncio.sleep(self._ttl // 2)
                except Exception as e:
                    logger.exception(f"heartbeat id:{self._lease_id}. error:{e}")
                finally:
                    await asyncio.sleep(min(5, self._ttl // 2))
        except asyncio.CancelledError:
            pass

    async def register(self, server_name: str, host: str, port: str, weight: int) -> None:
        key: str = f"{self.namespace}/{server_name}/{host}/{port}"
        value: str = json.dumps({"host": host, "port": port, "weight": weight})
        if not self._heartbeat_future:
            resp = await self._client.lease_grant(self._ttl, self._lease_id)
            self._lease_id = resp.ID
            self._heartbeat_future = asyncio.ensure_future(self._heartbeat())
        logger.info(f"register key: {key} value: {value}")
        await self._client.put(key, value, lease=self._lease_id)

    async def deregister(self, server_name: str, host: str, port: str) -> None:
        key: str = f"{self.namespace}/{server_name}/{host}/{port}"
        await self._client.delete_range(key)

    async def discovery(self, server_name: str) -> AsyncGenerator[dict, Any]:
        key: str = f"{self.namespace}/{server_name}"
        resp: Any = await self._client.range(key, prefix=True)
        if not resp.kvs:
            return
        else:
            for item in resp.kvs:
                yield json.loads(item.value.decode())

    async def watch(
        self,
        server_name: str,
        put_callback: List[Callable[[ETCD_EVENT_VALUE_DICT_TYPE], Any]],
        del_callback: List[Callable[[ETCD_EVENT_VALUE_DICT_TYPE], Any]],
    ) -> None:
        """etcd.AioClient not implemented api"""
        key: str = f"{self.namespace}/{server_name}"
        while True:
            try:
                async for i in self._client.watch_create(key, prefix=True):
                    resp_dict: dict = i._data
                    if "events" in resp_dict:
                        for event in resp_dict["events"]:
                            event_type = event.get("type", EventEventType.PUT)
                            if event_type == EventEventType.PUT:
                                run_callback: List[Callable] = put_callback
                                etcd_value_dict: ETCD_EVENT_VALUE_DICT_TYPE = {
                                    "key": event["kv"]["key"].decode(),
                                    "value": json.loads(event["kv"]["value"].decode()),
                                }
                            elif event_type == EventEventType.DELETE:
                                run_callback = del_callback
                                etcd_value_dict = {"key": event["kv"]["key"].decode(), "value": {}}
                            else:
                                continue

                            for fn in run_callback:
                                ret = fn(etcd_value_dict)
                                if inspect.iscoroutine(ret):
                                    await ret
                    elif "created" in resp_dict:
                        logging.info(f"watch {key} success")
            except Exception as e:
                logger.exception(f"watch etcd error:{e}")
            await asyncio.sleep(0.01)
