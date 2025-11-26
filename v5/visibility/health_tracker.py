"""Health tracking and stale detection."""

from __future__ import annotations

import time
from typing import Iterable

from v5.common import metrics
from v5.common.models import HealthStatus, PoolState


class HealthTracker:
    def __init__(self, stale_snapshot_ms: int, stale_stream_ms: int) -> None:
        self.stale_snapshot_ms = stale_snapshot_ms
        self.stale_stream_ms = stale_stream_ms

    def evaluate(self, pools: Iterable[PoolState]) -> list[HealthStatus]:
        now_ms = int(time.time() * 1000)
        statuses: list[HealthStatus] = []
        for p in pools:
            pool_addr = p.pool_meta.pool_address
            platform = p.pool_meta.platform
            network = p.pool_meta.network
            is_stale = False
            if p.last_snapshot_ms is not None and now_ms - p.last_snapshot_ms > self.stale_snapshot_ms:
                metrics.POOL_STALE_SNAPSHOT.labels(pool_addr, platform, network).set(1)
                is_stale = True
            else:
                metrics.POOL_STALE_SNAPSHOT.labels(pool_addr, platform, network).set(0)
            if p.last_ws_refresh_ms is not None and now_ms - p.last_ws_refresh_ms > self.stale_stream_ms:
                metrics.POOL_STALE_STREAM.labels(pool_addr, platform, network).set(1)
                is_stale = True
            else:
                metrics.POOL_STALE_STREAM.labels(pool_addr, platform, network).set(0)
            statuses.append(
                HealthStatus(
                    pool_address=pool_addr,
                    is_stale=is_stale,
                    last_snapshot_ms=p.last_snapshot_ms,
                    last_ws_ms=p.last_ws_refresh_ms,
                    has_data=True,
                )
            )
        return statuses

    def summary(self, pools: Iterable[PoolState]) -> dict:
        stats = self.evaluate(pools)
        total = len(stats)
        stale = sum(1 for s in stats if s.is_stale)
        healthy = total - stale
        return {"total": total, "stale": stale, "healthy": healthy}
