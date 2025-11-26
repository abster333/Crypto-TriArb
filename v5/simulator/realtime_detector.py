"""Continuous arbitrage detection loop."""

from __future__ import annotations

import asyncio
import time
import logging
import contextlib
import csv
from pathlib import Path
from collections import deque, defaultdict
from typing import Deque, List, Optional, Dict, Set

from v5.common import metrics
from v5.common.models import Opportunity, PoolState

log = logging.getLogger(__name__)


def price_from_sqrt(sqrt_price_x96: int | str, dec0: int, dec1: int) -> float | None:
    """Convert sqrtPriceX96 to human token1/token0 price.

    price_raw = (sqrtPriceX96 / 2**96) ** 2  (uses raw token units)
    price_human = price_raw * 10**(dec0 - dec1)

    Correct token ordering (token0/token1 matching on-chain pool) is critical;
    if token order is flipped, the scaling will be wrong by 10**(dec diff).
    """
    try:
        sqrt = int(str(sqrt_price_x96), 0)
        ratio = (sqrt / (2**96)) ** 2
        scale = 10 ** (dec0 - dec1)
        return ratio * scale
    except Exception:
        return None


class RealtimeArbDetector:
    def __init__(
        self,
        scenario_runner,
        *,
        min_roi_threshold: float = 0.001,
        check_interval_ms: int = 100,
        recent_limit: int = 50,
        executor=None,
    ):
        self.scenario_runner = scenario_runner
        self.min_roi = min_roi_threshold
        self.check_interval = check_interval_ms / 1000.0
        self.executor = executor
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._health_task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()
        self.recent: Deque[Opportunity] = deque(maxlen=recent_limit)
        self._pool_to_cycles: Dict[str, Set[str]] = defaultdict(set)
        self._pool_meta_by_addr = {p.pool_address.lower(): p for p in getattr(scenario_runner, "pools", [])}
        # Build pool→cycle mapping from expanded cycle variants
        for variant in getattr(scenario_runner, "cycle_variants", []) or []:
            cyc_label = "->".join(variant.tokens)
            for edge in variant.edges:
                self._pool_to_cycles[edge.pool.pool_address.lower()].add(cyc_label)
        self._ws_triggers = 0
        self._opps_detected = 0
        self._calc_durations_ms: Deque[float] = deque(maxlen=500)
        self._csv_file = None
        self._csv_writer = None

    async def start(self) -> None:
        if self._running:
            return
        # prepare csv logger
        path = Path("run_logs") / f"opportunities_{int(time.time())}.csv"
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            self._csv_file = open(path, "w", newline="", encoding="utf-8")
            self._csv_writer = csv.writer(self._csv_file)
            self._csv_writer.writerow(
                [
                    "ts_ms",
                    "ts_iso",
                    "cycle",
                    "roi",
                    "profit",
                    "legs",
                    "size_cap_token_in",
                    "leg_details",
                ]
            )
            log.info("OPP_CSV path=%s", path)
        except Exception as exc:  # noqa: BLE001
            log.warning("Failed to open opportunity CSV %s: %s", path, exc)

        self._running = True
        self._task = asyncio.create_task(self._detection_loop(), name="realtime-arb-detector")
        self._health_task = asyncio.create_task(self._health_loop(), name="realtime-arb-health")

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        if self._health_task:
            self._health_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._health_task
            self._health_task = None
        if self._csv_file:
            try:
                self._csv_file.flush()
                self._csv_file.close()
            except Exception:
                pass
            self._csv_file = None
            self._csv_writer = None

    async def _detection_loop(self) -> None:
        while self._running:
            start = time.time()
            await self._run_once(reason="timer")
            duration = time.time() - start
            await asyncio.sleep(max(0.0, self.check_interval - duration))

    async def on_state_update(self, state: PoolState, source: str = "ws") -> None:
        """Called by ingest when a pool state updates."""
        self._ws_triggers += 1
        await self._run_once(reason="ws", pool_address=state.pool_meta.pool_address, source=source, state_ts=state.timestamp_ms)

    async def _run_once(self, *, reason: str, pool_address: str | None = None, source: str = "", state_ts: int | None = None) -> None:
        start = time.time()
        async with self._lock:
            try:
                opportunities = await self.scenario_runner.run_once()
            except Exception as exc:  # noqa: BLE001
                log.warning("Detection loop failed: %s", exc)
                opportunities = []
        profitable = [o for o in opportunities if o.total_roi >= self.min_roi]
        for opp in profitable:
            await self._handle_opportunity(opp)
        duration = time.time() - start
        self._calc_durations_ms.append(duration * 1000)
        metrics.SIM_DURATION_SECONDS.observe(duration)
        metrics.SIM_OPPORTUNITIES.labels(cycle="any").inc(len(profitable))
        metrics.DETECTOR_TRIGGERS.labels(reason=reason).inc()
        if reason == "ws" and state_ts:
            metrics.WS_TO_SIM_SECONDS.observe(max(0.0, time.time() - (state_ts / 1000)))
        if pool_address:
            affected = list(self._pool_to_cycles.get(pool_address.lower(), []))
            log.info(
                "WS_TRIGGER pool=%s cycles=%d reason=%s source=%s opps=%d duration_ms=%.2f",
                pool_address[:8],
                len(affected),
                reason,
                source,
                len(profitable),
                duration * 1000,
            )
        else:
            cycle_count = len(getattr(self.scenario_runner, "cycle_variants", []))
            log.info(
                "RUN_RESULT reason=%s cycles=%d opps=%d duration_ms=%.2f",
                reason,
                cycle_count,
                len(profitable),
                duration * 1000,
            )

    async def _handle_opportunity(self, opp: Opportunity) -> None:
        self.recent.append(opp)
        self._opps_detected += 1
        try:
            from v5.visibility.metrics_exporter import publish_opportunity

            publish_opportunity(opp)
        except Exception:
            pass
        cycle_name = "->".join([leg.token_in for leg in opp.legs] + [opp.legs[0].token_in]) if opp.legs else "unknown"
        log.info("OPPORTUNITY cycle=%s roi=%.4f legs=%d profit=%.4f", cycle_name, opp.total_roi, len(opp.legs), opp.profit)
        # CSV log
        if self._csv_writer:
            leg_details = ";".join(
                f"{leg.token_in}->{leg.token_out}@{leg.pool}|fee={self._fee_for_pool(leg.pool)}|impact={getattr(leg, 'price_impact', '?')}"
                for leg in opp.legs
            )
            ts_ms = int(time.time() * 1000)
            ts_iso = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())
            try:
                self._csv_writer.writerow(
                    [
                        ts_ms,
                        ts_iso,
                        cycle_name,
                        opp.total_roi,
                        opp.profit,
                        len(opp.legs),
                        opp.size_cap if getattr(opp, "size_cap", None) is not None else "",
                        leg_details,
                    ]
                )
            except Exception:
                pass
        if self.executor:
            asyncio.create_task(self.executor.simulate_execution(opp))

    def recent_opportunities(self) -> List[Opportunity]:
        return list(self.recent)

    def _fee_for_pool(self, pool_addr: str) -> str:
        meta = self._pool_meta_by_addr.get(pool_addr.lower())
        return str(getattr(meta, "fee_tier", "?")) if meta else "?"

    def get_recent_opportunities(self, limit: int = 50) -> List[Opportunity]:
        return list(self.recent)[-limit:]

    async def _health_loop(self) -> None:
        while self._running:
            await asyncio.sleep(60)
            try:
                fresh, total = await self._count_fresh_pools()
                avg_calc = sum(self._calc_durations_ms) / len(self._calc_durations_ms) if self._calc_durations_ms else 0.0
                log.info(
                    "HEALTH pools_fresh=%d/%d ws_triggers_1m=%d opps_1m=%d avg_calc_ms=%.2f",
                    fresh,
                    total,
                    self._ws_triggers,
                    self._opps_detected,
                    avg_calc,
                )
            finally:
                self._ws_triggers = 0
                self._opps_detected = 0
                self._calc_durations_ms.clear()

    async def _count_fresh_pools(self, max_age_ms: int = 30_000) -> tuple[int, int]:
        reader = getattr(self.scenario_runner, "state_reader", None)
        pools = getattr(self.scenario_runner, "pools", [])
        if not reader or not pools:
            return 0, 0
        states = await reader.get_all_pool_states([p.pool_address for p in pools])
        now_ms = int(time.time() * 1000)
        fresh = 0
        for st in states.values():
            ts = st.get("last_ws_refresh_ms") or st.get("last_snapshot_ms") or st.get("timestamp_ms") or 0
            if ts and now_ms - ts <= max_age_ms:
                fresh += 1
        return fresh, len(pools)
