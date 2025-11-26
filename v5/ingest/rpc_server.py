"""Minimal batch RPC helper for pool_state fetches (no V4 deps)."""

from __future__ import annotations

import asyncio
import time
import contextlib
import json
from typing import Any, Dict, Optional, Sequence, Tuple, List

import aiohttp
import logging

from v5.common.config import Settings
from v5.common import metrics
from v5.ingest.circuit_breaker import CircuitBreaker

log = logging.getLogger(__name__)


class BatchRpcServer:
    def __init__(
        self,
        *,
        max_calls_per_second: int = 2,
        max_batch_size: int = 10,
        max_retries: int = 3,
        timeout_seconds: float = 10.0,
    ) -> None:
        self.settings = Settings()
        self._max_qps = max_calls_per_second
        self._max_batch = max_batch_size
        self._max_retries = max_retries
        self._timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        self._queue: asyncio.Queue = asyncio.Queue()
        self._task: asyncio.Task | None = None
        self._last_call_ts = 0.0
        self._session: aiohttp.ClientSession | None = None
        self._breaker = CircuitBreaker()

    async def start(self) -> None:
        if self._task:
            return
        self._session = aiohttp.ClientSession(timeout=self._timeout)
        self._task = asyncio.create_task(self._run(), name="v5-batch-rpc")

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        if self._session:
            await self._session.close()
            self._session = None

    async def submit_pool_state(self, network: str, address: str, *, block_tag: Optional[str] = None) -> Dict[str, Any] | None:
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        await self._queue.put(("pool_state", network, address.lower(), block_tag, fut))
        return await fut

    async def submit_call(self, network: str, method: str, params: list[Any]) -> Any:
        """Submit a generic JSON-RPC call."""
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        await self._queue.put(("call", network, method, params, fut))
        return await fut

    async def _run(self) -> None:
        while True:
            job = await self._queue.get()
            batch = [job]
            try:
                while len(batch) < self._max_batch:
                    batch.append(self._queue.get_nowait())
            except asyncio.QueueEmpty:
                pass
            await self._process_batch(batch)

    async def _process_batch(self, batch: Sequence[tuple]) -> None:
        now = time.monotonic()
        min_interval = 1.0 / float(self._max_qps)
        since_last = now - self._last_call_ts
        if since_last < min_interval:
            await asyncio.sleep(min_interval - since_last)
        self._last_call_ts = time.monotonic()

        jobs_by_net: Dict[str, list] = {}
        for job in batch:
            net = job[1].lower()
            jobs_by_net.setdefault(net, []).append(job)

        for net, jobs in jobs_by_net.items():
            url = self._rpc_url_for_network(net)
            if not url or not self._session:
                for job in jobs:
                    fut = job[-1]
                    if not fut.done():
                        fut.set_result(None)
                continue
            # Split pool_state vs generic calls
            pool_jobs = [j for j in jobs if j[0] == "pool_state"]
            call_jobs = [j for j in jobs if j[0] == "call"]

            # Build payload
            payload: List[Dict[str, Any]] = []
            id_map: Dict[int, Tuple[str, str]] = {}

            def add_call(method: str, params: list[Any], label: str):
                idx = len(payload) + 1
                payload.append({"jsonrpc": "2.0", "id": idx, "method": method, "params": params})
                id_map[idx] = (label, method)
                return idx

            addresses: List[str] = []
            added_block = False
            for job in pool_jobs:
                _, _, addr, block_tag, _ = job
                addresses.append(addr)
                tag = block_tag or "latest"
                add_call("eth_call", [{"to": addr, "data": "0x3850c7bd"}, tag], f"slot0:{addr}")
                add_call("eth_call", [{"to": addr, "data": "0x1a686502"}, tag], f"liq:{addr}")
                if not added_block:
                    add_call("eth_blockNumber", [], "block")
                    added_block = True

            for job in call_jobs:
                _, _, method, params, _ = job
                add_call(method, params, f"call:{method}")

            if not payload:
                continue

            async def _do_post():
                start = time.time()
                async with self._session.post(url, json=payload) as resp:
                    status = resp.status
                    text = await resp.text()
                    if status >= 400:
                        log.warning(
                            "RPC_HTTP_ERROR network=%s status=%s payload=%dbytes", net, status, len(text or ""),
                        )
                        if status == 429:
                            log.warning("RPC_RATE_LIMIT network=%s", net)
                    try:
                        data_local = json.loads(text)
                    except Exception:
                        data_local = None
                metrics.RPC_LATENCY_SECONDS.labels(network=net, method="batch").observe(time.time() - start)
                return data_local

            data = None
            attempt = 0
            while attempt < self._max_retries:
                attempt += 1
                try:
                    data = await self._breaker.call(_do_post)
                    break
                except Exception:
                    metrics.INGEST_ERRORS.labels(type="rpc_retry").inc()
                    await asyncio.sleep(min(2**attempt, 5) * 0.1)
            if data is None:
                metrics.INGEST_ERRORS.labels(type="rpc_failed").inc()

            # Map results
            result_by_label: Dict[str, Any] = {}
            if isinstance(data, list):
                for item in data:
                    if not isinstance(item, dict):
                        continue
                    _id = item.get("id")
                    label_method = id_map.get(int(_id)) if _id else None
                    if label_method:
                        label, method_name = label_method
                        if "error" in item:
                            err = item.get("error") or {}
                            log.warning(
                                "RPC_ERROR network=%s id=%s code=%s msg=%s", net, _id, err.get("code"), err.get("message")
                            )
                        result_by_label[label] = item.get("result")
                        metrics.INGEST_RPC_CALLS.labels(network=net, method=method_name).inc()
            
            # Resolve futures
            block_hex = result_by_label.get("block")
            block_number = int(block_hex, 16) if isinstance(block_hex, str) and block_hex.startswith("0x") else 0
            for job in pool_jobs:
                _, _, addr, _, fut = job
                res = {
                    "slot0": result_by_label.get(f"slot0:{addr}"),
                    "liquidity": result_by_label.get(f"liq:{addr}"),
                    "block_number": block_number,
                }
                if not fut.done():
                    fut.set_result(res)
            for job in call_jobs:
                _, _, method, _, fut = job
                res = result_by_label.get(f"call:{method}")
                if not fut.done():
                    fut.set_result(res)

    def _rpc_url_for_network(self, network: str) -> Optional[str]:
        net = network.lower()
        if net in ("eth", "ethereum"):
            return self.settings.ethereum_rpc_url
        if net in ("bsc", "bnb", "bnb chain"):
            return self.settings.bsc_rpc_url
        return None


__all__ = ["BatchRpcServer"]
