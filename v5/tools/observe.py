"""Run a 10-minute observation of the V5 pipeline and print summary metrics."""

from __future__ import annotations

import asyncio
import logging
from decimal import Decimal

from v5.common.config import Settings
from v5.ingest.ingest_service import IngestService
from v5.ingest.rpc_server import BatchRpcServer
from v5.state_store.hot_cache import HotCache
from v5.state_store.snapshot_store import SnapshotStore
from v5.state_store.state_reader import StateReader
from v5.simulator.scenario_runner import ScenarioRunner
from v5.simulator.realtime_detector import RealtimeArbDetector
import os


async def main(run_seconds: int = 600) -> None:
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    settings = Settings()
    await settings.ensure_manifests(settings.generate_manifests)
    pools = settings.load_pools()
    tokens = settings.load_tokens()

    hot = HotCache(settings.redis_url)
    snap = SnapshotStore(settings.redis_url)
    await hot.start(); await snap.start()
    reader = StateReader(hot, snap)

    rpc = BatchRpcServer(max_calls_per_second=3, max_batch_size=10)
    await rpc.start()
    ingest = IngestService(
        rpc,
        pools,
        hot,
        snap,
        uniswap_ws_url=settings.ethereum_ws_url or settings.alchemy_ws_url or "",
        pancake_ws_url=settings.bsc_ws_url or "",
        resync_interval_seconds=settings.resync_interval_seconds,
        enable_poll_fallback=settings.enable_poll_fallback,
    )

    runner = ScenarioRunner(
        reader,
        tokens,
        pools,
        initial_amount=Decimal(str(settings.initial_amount)),
        min_roi_threshold=0,
        enforce_freshness=False,
    )
    detector = RealtimeArbDetector(
        runner,
        min_roi_threshold=settings.strategy_tri_threshold_bps / 10_000.0,
    )
    ingest.state_update_callbacks.append(detector.on_state_update)

    await ingest.start()
    await detector.start()

    print(f"Observing for {run_seconds}s over {len(pools)} pools...")
    await asyncio.sleep(run_seconds)

    # summary
    import statistics
    import time

    states = await reader.get_all_pool_states([p.pool_address for p in pools])
    now = time.time() * 1000
    ages_ms: list[int] = []
    ws_ages: list[int] = []
    snap_ages: list[int] = []

    fresh_30 = fresh_90 = fresh_300 = 0

    for st in states.values():
        payload = st.get("payload", st)
        ws_ts = payload.get("last_ws_refresh_ms") or 0
        snap_ts = payload.get("last_snapshot_ms") or 0
        ts = ws_ts or snap_ts or payload.get("timestamp_ms") or 0
        if ts:
            age = int(now - ts)
            ages_ms.append(age)
            if ws_ts:
                ws_ages.append(int(now - ws_ts))
            if snap_ts:
                snap_ages.append(int(now - snap_ts))
            if age <= 30_000:
                fresh_30 += 1
            if age <= 90_000:
                fresh_90 += 1
            if age <= 300_000:
                fresh_300 += 1

    def _summary(arr: list[int]) -> str:
        if not arr:
            return "n/a"
        arr_sorted = sorted(arr)
        return (
            f"min={arr_sorted[0]:.0f}ms "
            f"p50={statistics.median(arr_sorted):.0f}ms "
            f"p95={arr_sorted[int(0.95*len(arr_sorted))-1]:.0f}ms "
            f"max={arr_sorted[-1]:.0f}ms"
        )

    print(f"Fresh pools <=30s: {fresh_30}/{len(pools)} | <=90s: {fresh_90}/{len(pools)} | <=300s: {fresh_300}/{len(pools)}")
    print(f"Data age (any ts): {_summary(ages_ms)}")
    print(f"WS age: {_summary(ws_ages)}")
    print(f"Snapshot age: {_summary(snap_ages)}")
    print(f"Recent opportunities: {len(detector.recent_opportunities())}")

    await detector.stop()
    await ingest.stop()
    await rpc.stop()
    await hot.close()
    await snap.close()


if __name__ == "__main__":
    asyncio.run(main())
