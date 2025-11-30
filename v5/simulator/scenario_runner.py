"""Runs simulator cycles using current state store snapshots."""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Dict, List

import time
import logging

from v5.common import metrics
from v5.common.models import PoolState, Opportunity, Token, PoolMeta, OpportunityLeg
from v5.simulator.graph import TokenGraph
from v5.simulator.roi_calc import compute_roi
from v5.state_store.state_reader import StateReader

MAX_DATA_AGE_MS = 60_000

log = logging.getLogger(__name__)

class ScenarioRunner:
    def __init__(
        self,
        state_reader: StateReader,
        tokens: Dict[str, Token],
        pools: List[PoolMeta],
        initial_amount: Decimal = Decimal("1"),
        min_roi_threshold: float = 0.0,
        enforce_freshness: bool = False,
    ) -> None:
        import logging
        log = logging.getLogger(__name__)
        log.info("=" * 60)
        log.info("SCENARIO RUNNER INITIALIZATION")
        log.info("=" * 60)
        log.info("Tokens provided: %d", len(tokens))
        log.info("Token symbols: %s", sorted(list(tokens.keys())[:20]))
        log.info("Pools provided: %d", len(pools))

        self.state_reader = state_reader
        self.tokens = tokens
        self.pools = pools
        self.graph = TokenGraph(pools)

        token_symbols = list(tokens.keys())
        log.info("Searching for cycle variants using %d token symbols", len(token_symbols))
        self.cycle_variants = self.graph.find_triangular_cycle_variants(token_symbols)
        # Backward/compat: keep a simple token-cycle list for any legacy users
        self.cycles = [cv.tokens for cv in self.cycle_variants]
        log.info("RESULT: %d cycle variants found (directed × fee tiers)", len(self.cycle_variants))
        if self.cycle_variants:
            for i, variant in enumerate(self.cycle_variants[:5]):
                hops = [f"{e.base}->{e.quote}@{int(e.fee*1_000_000)}bps" for e in variant.edges]
                log.info("  [%d] %s -> %s -> %s -> %s | %s", i, *variant.tokens, variant.tokens[0], ", ".join(hops))
        else:
            log.error("CRITICAL: NO CYCLE VARIANTS FOUND!")
            if pools:
                log.error("First pool tokens: %s / %s", pools[0].token0, pools[0].token1)

        self.initial_amount = initial_amount
        self.min_roi = min_roi_threshold
        self.enforce_freshness = enforce_freshness
        metrics.ACTIVE_CYCLES.set(len(self.cycle_variants))

    async def run_once(self) -> List[Opportunity]:
        pool_states_raw = await self.state_reader.get_all_pool_states([p.pool_address for p in self.pools])
        pool_states: Dict[str, PoolState] = {}
        for meta in self.pools:
            raw = pool_states_raw.get(meta.pool_address) or {}
            if not raw:
                continue
            # reconstruct PoolState
            pool_states[meta.pool_address] = PoolState(
                pool_meta=meta,
                sqrt_price_x96=str(raw.get("sqrt_price_x96", "1")),
                tick=int(raw.get("tick", 0)),
                liquidity=str(raw.get("liquidity", "1")),
                block_number=int(raw.get("block_number", 0)),
                timestamp_ms=int(raw.get("timestamp_ms", 0)),
            )
        opportunities: List[Opportunity] = []
        for variant in self.cycle_variants:
            cyc_label = "->".join(variant.tokens)
            cyc_start = time.time()
            edges = variant.edges
            if len(edges) < 3:
                metrics.CYCLE_SIMULATION_TIME.labels(cycle=cyc_label).observe(time.time() - cyc_start)
                continue
            valid, reason = self._validate_cycle(edges, pool_states)
            if not valid:
                metrics.CYCLE_SKIP.labels(cycle=cyc_label, reason=reason).inc()
                metrics.CYCLE_SIMULATION_TIME.labels(cycle=cyc_label).observe(time.time() - cyc_start)
                if reason in ("stale_data", "missing_state", "invalid_state"):
                    # Too chatty in normal operation; keep at debug for targeted troubleshooting.
                    log.debug("STALE_OR_MISSING cycle=%s reason=%s", cyc_label, reason)
                continue
            legs: List[OpportunityLeg] = []
            token_in = variant.tokens[0]
            for edge in edges:
                legs.append(
                    OpportunityLeg(
                        pool=edge.pool.pool_address,
                        token_in=token_in,
                        token_out=edge.quote,
                        amount_in=1.0,
                        amount_out=1.0,
                        price_impact=0.0,
                    )
                )
                token_in = edge.quote
            opp = compute_roi(legs, pool_states, self.initial_amount, self.min_roi)
            if opp:
                metrics.CYCLE_ROI_HISTOGRAM.labels(cycle=cyc_label).observe(opp.total_roi)
                opportunities.append(opp)
            metrics.CYCLE_CALC_SECONDS.labels(cycle=cyc_label).observe(time.time() - cyc_start)
            metrics.CYCLE_SIMULATION_TIME.labels(cycle=cyc_label).observe(time.time() - cyc_start)
        opportunities.sort(key=lambda o: o.total_roi, reverse=True)
        return opportunities

    async def snapshot_cycles(self, min_roi: float = -1_000_000.0) -> List[dict]:
        """
        Return a snapshot of all cycle variants with ROI and status, regardless of profitability.
        Intended for UI/debug visibility.
        """
        pool_states_raw = await self.state_reader.get_all_pool_states([p.pool_address for p in self.pools])
        pool_states: Dict[str, PoolState] = {}
        for meta in self.pools:
            raw = pool_states_raw.get(meta.pool_address) or {}
            if not raw:
                continue
            pool_states[meta.pool_address] = PoolState(
                pool_meta=meta,
                sqrt_price_x96=str(raw.get("sqrt_price_x96", "1")),
                tick=int(raw.get("tick", 0)),
                liquidity=str(raw.get("liquidity", "1")),
                block_number=int(raw.get("block_number", 0)),
                timestamp_ms=int(raw.get("timestamp_ms", 0)),
                last_snapshot_ms=raw.get("last_snapshot_ms"),
                last_ws_refresh_ms=raw.get("last_ws_refresh_ms"),
                stream_ready=raw.get("stream_ready", False),
            )

        results: List[dict] = []
        now_ms = int(time.time() * 1000)

        for variant in self.cycle_variants:
            cyc_label = "->".join(variant.tokens)
            edges = variant.edges
            status = "ok"
            reason = ""
            opp_data = None
            quoter_data = None

            valid, reason = self._validate_cycle(edges, pool_states)
            if not valid:
                status = "skipped"
            else:
                legs: List[OpportunityLeg] = []
                token_in = variant.tokens[0]
                for edge in edges:
                    legs.append(
                        OpportunityLeg(
                            pool=edge.pool.pool_address,
                            token_in=token_in,
                            token_out=edge.quote,
                            amount_in=1.0,
                            amount_out=1.0,
                            price_impact=0.0,
                        )
                    )
                    token_in = edge.quote
                opp = compute_roi(legs, pool_states, self.initial_amount, min_roi)
                if opp:
                    opp_data = opp
                    # attach quoter details if present from detector cache for matching cycle
                    if hasattr(self, "detector_quoter_cache"):
                        quoter_data = self.detector_quoter_cache.get(cyc_label)
                else:
                    status = "no_opp"

            # gather freshness/age info (max age across legs)
            ages = []
            for edge in edges:
                st = pool_states.get(edge.pool.pool_address)
                if st:
                    ts = st.last_ws_refresh_ms or st.last_snapshot_ms or st.timestamp_ms or 0
                    if ts:
                        ages.append(now_ms - ts)
            max_age_ms = max(ages) if ages else None

            result = {
                "cycle": cyc_label,
                "status": status,
                "reason": reason,
                "max_age_ms": max_age_ms,
                "roi": opp_data.total_roi if opp_data else None,
                "roi_bps": opp_data.total_roi * 10_000 if opp_data else None,
                "profit": opp_data.profit if opp_data else None,
                "size_cap": opp_data.size_cap if opp_data else None,
                "legs": [],
                "quoter": quoter_data,
            }

            if opp_data:
                result["legs"] = [
                    {
                        "pool": leg.pool,
                        "token_in": leg.token_in,
                        "token_out": leg.token_out,
                        "amount_in": leg.amount_in,
                        "amount_out": leg.amount_out,
                        "price_impact": leg.price_impact,
                    }
                    for leg in opp_data.legs
                ]
            else:
                # provide minimal leg info even when opp is missing
                result["legs"] = [
                    {
                        "pool": edge.pool.pool_address,
                        "token_in": edge.base,
                        "token_out": edge.quote,
                    }
                    for edge in edges
                ]

            results.append(result)

        return results

    def _validate_cycle(self, edges, pool_states: Dict[str, PoolState]) -> tuple[bool, str]:
        now_ms = int(time.time() * 1000)
        for edge in edges:
            st = pool_states.get(edge.pool.pool_address)
            if not st:
                return False, "missing_state"
            if self.enforce_freshness:
                ts = st.last_ws_refresh_ms or st.last_snapshot_ms or st.timestamp_ms or 0
                age = now_ms - ts if ts else MAX_DATA_AGE_MS + 1
                if age > MAX_DATA_AGE_MS:
                    return False, "stale_data"
            try:
                if int(st.sqrt_price_x96) == 0 or int(st.liquidity) == 0:
                    return False, "invalid_state"
            except Exception:
                return False, "invalid_state"
        return True, ""
