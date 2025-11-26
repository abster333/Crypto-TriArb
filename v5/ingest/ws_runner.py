"""Rebuilt log-driven WS runner with backpressure and metrics."""

from __future__ import annotations

import asyncio
import logging
import time
import contextlib
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, List, Optional

from v5.common import metrics
from v5.common.models import PoolMeta, PoolState
from v5.ingest.rpc_server import BatchRpcServer
from v5.ingest.ws_adapters import WsAdapter
from v5.ingest.ws_types import WsEvent
from v5.simulator.realtime_detector import price_from_sqrt

log = logging.getLogger(__name__)

MAX_PENDING = 1024


@dataclass(slots=True)
class PoolBuffer:
    snapshot_block: Optional[int] = None
    pending: Deque[WsEvent] = field(default_factory=lambda: deque(maxlen=MAX_PENDING))


class WsRunner:
    """Consumes swap logs per network and keeps Redis state fresh."""

    def __init__(self, rpc: BatchRpcServer, pools: List[PoolMeta], queue: asyncio.Queue[PoolState], adapters: List[WsAdapter], chunk_size: int = 30):
        self.rpc = rpc
        self.pools = {p.pool_address.lower(): p for p in pools}
        self.queue = queue
        self._buffers: Dict[str, PoolBuffer] = {addr: PoolBuffer() for addr in self.pools}
        self._consume_task: asyncio.Task | None = None
        self._event_queue: asyncio.Queue[WsEvent] = asyncio.Queue(maxsize=MAX_PENDING)
        self.adapters = adapters
        self._adapter_tasks: List[asyncio.Task] = []
        self.chunk_size = max(1, chunk_size)

    async def start(self) -> None:
        addresses = list(self.pools.keys())
        for adapter in self.adapters:
            await adapter.connect()
            for i in range(0, len(addresses), self.chunk_size):
                batch = addresses[i : i + self.chunk_size]
                await adapter.subscribe(batch)
            self._adapter_tasks.append(asyncio.create_task(self._adapter_loop(adapter)))
        self._consume_task = asyncio.create_task(self._consume(), name="v5-ws-consume")

    async def stop(self) -> None:
        if self._consume_task:
            self._consume_task.cancel()
            try:
                await self._consume_task
            except asyncio.CancelledError:
                pass
        for task in self._adapter_tasks:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        # Cleanly close WS connections to avoid dangling sessions/credits
        for adapter in self.adapters:
            with contextlib.suppress(Exception):
                await adapter.close()

    async def handle_raw_swap(self, pool_address: str, block_number: Optional[int]) -> None:
        """Deprecated entry point; retained for compatibility."""
        evt = WsEvent(pool=pool_address.lower(), block_number=block_number, ts_ms=int(time.time() * 1000), sqrt_price_x96=0, liquidity=0, tick=0)
        await self._enqueue_event(evt, self._network_for_pool(pool_address))

    async def _consume(self) -> None:
        while True:
            evt = await self._event_queue.get()
            buf = self._buffers.get(evt.pool)
            if not buf:
                continue
            buf.pending.append(evt)
            log.debug("WS_EVENT dequeued pool=%s block=%s", evt.pool[:8], evt.block_number)
            await self._maybe_replay(evt.pool, buf)

    async def _maybe_replay(self, pool_addr: str, buf: PoolBuffer) -> None:
        meta = self.pools[pool_addr]
        if buf.snapshot_block is None:
            buf.snapshot_block = 0
        events = list(buf.pending)
        buf.pending.clear()
        for evt in events:
            state = self._state_from_event(meta, evt)
            if state:
                metrics.INGEST_WS_BACKPRESSURE.labels(network=meta.network).set(self._event_queue.qsize())
                await self.queue.put(state)
                log.debug(
                    "WS_STATE enqueued pool=%s block=%s sqrt=%s liq=%s tick=%s ts=%s",
                    meta.pool_address[:8],
                    evt.block_number,
                    state.sqrt_price_x96,
                    state.liquidity,
                    state.tick,
                    state.last_ws_refresh_ms,
                )
            else:
                metrics.INGEST_ERRORS.labels(type="ws_decode_state").inc()

    def _state_from_event(self, meta: PoolMeta, evt: WsEvent) -> PoolState | None:
        """Translate WS swap event into PoolState without extra RPC."""
        try:
            price = price_from_sqrt(evt.sqrt_price_x96, meta.dec0, meta.dec1)
            return PoolState(
                pool_meta=meta,
                sqrt_price_x96=str(evt.sqrt_price_x96),
                tick=int(evt.tick),
                liquidity=str(evt.liquidity),
                liquidity_int=int(evt.liquidity),
                price=price,
                block_number=evt.block_number or 0,
                timestamp_ms=evt.ts_ms,
                last_ws_refresh_ms=evt.ts_ms,
                stream_ready=True,
            )
        except Exception as exc:
            log.warning("WS_STATE_DECODE_FAIL pool=%s network=%s err=%s", meta.pool_address[:8], meta.network, exc)
            return None

    def _network_for_pool(self, pool: str) -> str:
        meta = self.pools.get(pool.lower())
        return meta.network if meta else "unknown"

    async def _adapter_loop(self, adapter: WsAdapter) -> None:
        while True:
            try:
                evt = await adapter.recv_event()
                if evt:
                    await self._enqueue_event(evt, self._network_for_pool(evt.pool))
            except Exception:
                await asyncio.sleep(1)

    async def _enqueue_event(self, evt: WsEvent, network: str) -> None:
        try:
            self._event_queue.put_nowait(evt)
            log.debug("WS_EVENT enqueue pool=%s block=%s ts=%s network=%s", evt.pool[:8], evt.block_number, evt.ts_ms, network)
        except asyncio.QueueFull:
            metrics.INGEST_ERRORS.labels(type="ws_backpressure_drop").inc()
            log.warning("WS_EVENT drop (backpressure) pool=%s network=%s qsize=%s", evt.pool[:8], network, self._event_queue.qsize())
        finally:
            metrics.INGEST_WS_BACKPRESSURE.labels(network=network).set(self._event_queue.qsize())


__all__ = ["WsRunner"]
