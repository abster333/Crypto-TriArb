"""Top-level ingest orchestrator wiring pollers, WS adapters, and state store."""

from __future__ import annotations

import asyncio
import contextlib
from asyncio import Queue
from typing import Iterable, List

import logging

from v5.common.models import PoolMeta, PoolState
from v5.ingest.pool_poller import PoolPoller, UniswapV3Poller, PancakeV3Poller
from v5.ingest.rpc_server import BatchRpcServer
from v5.ingest.ws_runner import WsRunner
from v5.ingest.ws_adapters import UniswapV3WsAdapter, PancakeV3WsAdapter
from v5.state_store.hot_cache import HotCache
from v5.state_store.snapshot_store import SnapshotStore

log = logging.getLogger(__name__)


class IngestService:
    def __init__(
        self,
        rpc: BatchRpcServer,
        polls: Iterable[PoolMeta],
        hot_cache: HotCache,
        snapshots: SnapshotStore,
        uniswap_ws_url: str | None = None,
        pancake_ws_url: str | None = None,
        resync_interval_seconds: int = 600,
        enable_poll_fallback: bool = False,
        ws_sub_batch_size: int = 30,
    ):
        self.rpc = rpc
        self.pools = list(polls)
        self.out_queue: Queue[PoolState] = Queue()
        self.hot_cache = hot_cache
        self.snapshots = snapshots
        self._tasks: List[asyncio.Task] = []
        self.state_update_callbacks: List = []
        self.resync_interval_seconds = resync_interval_seconds
        self.enable_poll_fallback = enable_poll_fallback
        self._drain_writes = 0

        # Pollers by network
        uni_pools = [p for p in self.pools if p.network.upper() == "ETH"]
        bsc_pools = [p for p in self.pools if p.network.upper() == "BSC"]
        self.pollers: List[PoolPoller] = []
        if uni_pools:
            self.pollers.append(UniswapV3Poller(rpc, uni_pools))
        if bsc_pools:
            self.pollers.append(PancakeV3Poller(rpc, bsc_pools))

        adapters = []
        if uniswap_ws_url and uniswap_ws_url.startswith("ws") and uni_pools:
            adapters.append(UniswapV3WsAdapter(uniswap_ws_url))
        elif uniswap_ws_url and not uniswap_ws_url.startswith("ws"):
            log.warning("Skipping ETH WS adapter; invalid ws url scheme: %s", uniswap_ws_url)
        if pancake_ws_url and pancake_ws_url.startswith("ws") and bsc_pools:
            adapters.append(PancakeV3WsAdapter(pancake_ws_url))
        elif pancake_ws_url and not pancake_ws_url.startswith("ws"):
            log.warning("Skipping BSC WS adapter; invalid ws url scheme: %s", pancake_ws_url)
        self.ws_runner = WsRunner(rpc, self.pools, self.out_queue, adapters, chunk_size=ws_sub_batch_size)

    async def start(self) -> None:
        await self.rpc.start()
        await self.snapshots.start()
        await self.hot_cache.start()
        for poller in self.pollers:
            await poller.start()
        await self.ws_runner.start()
        # Periodic resync loop (also acts as initial snapshot)
        self._tasks.append(asyncio.create_task(self._resync_loop(), name="v5-resync-loop"))
        # Optional legacy poll loop (disabled by default)
        if self.enable_poll_fallback:
            self._tasks.append(asyncio.create_task(self._poll_loop(), name="v5-poll-loop"))
        self._tasks.append(asyncio.create_task(self._drain_out_queue(), name="v5-out-drain"))

    async def stop(self) -> None:
        # allow drain loop to exit cleanly
        try:
            await self.out_queue.put(None)  # type: ignore[arg-type]
        except Exception:
            pass
        for task in self._tasks:
            task.cancel()
        for poller in self.pollers:
            await poller.stop()
        await self.ws_runner.stop()
        for task in self._tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task
        await self.ws_runner.stop()
        await self.rpc.stop()
        await self.hot_cache.close()
        await self.snapshots.close()

    async def _poll_loop(self) -> None:
        while True:
            for poller in self.pollers:
                snapshots = await poller.poll_once()
                for snap in snapshots:
                    await self.out_queue.put(snap)
            await asyncio.sleep(min(p.poll_interval for p in self.pollers) if self.pollers else 5)

    async def _resync_loop(self) -> None:
        """Periodic full sweep via RPC; also runs immediately on start."""
        # Initial sweep
        await self._run_resync_once()
        while True:
            await asyncio.sleep(self.resync_interval_seconds)
            await self._run_resync_once()

    async def _run_resync_once(self) -> None:
        for poller in self.pollers:
            snapshots = await poller.poll_once()
            for snap in snapshots:
                await self.out_queue.put(snap)

    async def _drain_out_queue(self) -> None:
        while True:
            state = await self.out_queue.get()
            if state is None:
                break
            try:
                await self.hot_cache.write_pool_state(state)
                self._drain_writes += 1
                if self._drain_writes <= 5 or self._drain_writes % 100 == 0:
                    log.info(
                        "DRAIN_WRITE_HOT count=%d pool=%s stream=%s block=%s",
                        self._drain_writes,
                        state.pool_meta.pool_address[:8],
                        state.stream_ready,
                        state.block_number,
                    )
            except Exception:  # noqa: BLE001
                log.exception("hot_cache write failed")
            try:
                await self.snapshots.write_if_changed(state)
            except Exception:  # noqa: BLE001
                log.exception("snapshot write failed")
            for cb in self.state_update_callbacks:
                try:
                    res = cb(state)
                    if asyncio.iscoroutine(res):
                        await res
                except Exception:
                    log.exception("State update callback failed")

    def health(self) -> dict:
        return {
            "pollers": len(self.pollers),
            "ws_running": True,
        }
