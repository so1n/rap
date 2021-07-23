import asyncio
import json
import logging
from typing import Any, AsyncGenerator, Dict, List, Optional, Tuple

from consul import Check
from consul.aio import Consul
from .bass import BaseCoordinator

logger: logging.Logger = logging.getLogger()


class ConsulClient(BaseCoordinator):
    def __init__(
        self,
        namespace: str = "rap",
        ttl: int = 10,
        host: str = '127.0.0.1',
        port: int = 8500,
        token: Optional[str] = None,
        scheme: str = 'http',
        consistency: str = 'default',
        dc: Optional[str] = None,
        verify: bool = True,
        cert: Optional[str] = None
    ):
        self._ttl: int = ttl
        self.namespace: str = namespace
        self._heartbeat_future_dict: Dict[str, asyncio.Future] = {}
        self._client: Consul = Consul(
            host=host,
            port=port,
            token=token,
            scheme=scheme,
            consistency=consistency,
            dc=dc,
            verify=verify,
            cert=cert
        )

    async def stop(self) -> None:
        for service_id, future in self._heartbeat_future_dict.items():
            if not future.done() and not future.cancelled():
                future.cancel()
        self._client.close()

    async def _heartbeat(self, service_id: str) -> None:
        try:
            while True:
                logger.debug(f"heartbeat by consul, id: {service_id}")
                try:
                    await self._client.agent.check.ttl_pass(service_id)
                    await asyncio.sleep(self._ttl // 2)
                except Exception as e:
                    logger.exception(f"heartbeat id:{service_id}. error:{e}")
                finally:
                    await asyncio.sleep(min(5, self._ttl // 2))
        except asyncio.CancelledError:
            pass

    async def register(self, server_name: str, host: str, port: str, weight: int) -> None:
        service_id: str = f"{self.namespace}/{server_name}/{host}"
        await self._client.kv.put(f"{service_id}/{port}", json.dumps({"weight": weight}))
        await self._client.agent.service.register(
            f"{self.namespace}/{server_name}",
            service_id=service_id,
            address=host,
            port=int(port),
            check=Check.ttl(f"{self._ttl}s"),
        )
        if service_id not in self._heartbeat_future_dict:
            self._heartbeat_future_dict[service_id] = asyncio.ensure_future(self._heartbeat(f"service:{service_id}"))

    async def deregister(self, server_name: str, host: str, port: str) -> None:
        service_id: str = f"{self.namespace}/{server_name}/{host}"
        await self._client.kv.delete(f"{service_id}/{port}")
        await self._client.agent.service.deregister(service_id)
        await self._client.agent.check.deregister(service_id)

    async def discovery(self, server_name: str) -> AsyncGenerator[dict, Any]:
        resp: Tuple[str, List[Dict[str, Any]]] = await self._client.catalog.service(f"{self.namespace}/{server_name}")
        for item in resp[1]:
            kv_resp: Tuple[str, List[Dict[str, Any]]] = await self._client.kv.get(
                f"{item['ServiceID']}/{item['ServicePort']}"
            )
            result_dict: dict = json.loads(kv_resp[1]["Value"].decode())
            result_dict["host"] = item["ServiceAddress"]
            result_dict["port"] = item["ServicePort"]
            yield result_dict

    async def watch(self, server_name: str) -> AsyncGenerator[Dict[str, Dict[str, Any]], Any]:
        index: Optional[str] = None
        while True:
            resp: Tuple[str, List[Dict[str, Any]]] = await self._client.catalog.service(
                f"{self.namespace}/{server_name}", index=index, wait=f"{self._ttl}s"
            )
            index = resp[0]

            conn_dict: Dict[str, Dict] = {}
            for item in resp[1]:
                host: str = item["ServiceAddress"]
                port: int = item["ServicePort"]
                kv_resp: Tuple[str, List[Dict[str, Any]]] = await self._client.kv.get(
                    f"{item['ServiceID']}/{item['ServicePort']}"
                )
                result_dict: dict = json.loads(kv_resp[1]["Value"].decode())
                result_dict["host"] = host
                result_dict["port"] = port
                conn_dict[f"{host}_{port}"] = result_dict
            yield conn_dict
