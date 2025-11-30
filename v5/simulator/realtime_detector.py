"""Continuous arbitrage detection loop."""

from __future__ import annotations

import asyncio
import time
import logging
import contextlib
import json
import aiohttp
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
        # Token lookup for quoter (symbol -> Token object)
        self._tokens = getattr(scenario_runner, "tokens", {})
        # Build pool→cycle mapping from expanded cycle variants
        for variant in getattr(scenario_runner, "cycle_variants", []) or []:
            cyc_label = "->".join(variant.tokens)
            for edge in variant.edges:
                self._pool_to_cycles[edge.pool.pool_address.lower()].add(cyc_label)
        self._ws_triggers = 0
        self._opps_detected = 0
        self._calc_durations_ms: Deque[float] = deque(maxlen=500)
        self._jsonl_file = None
        # Cache the full set of opportunities from the last run (including non-profitable)
        self._last_run_opps: List[Opportunity] = []

    async def start(self) -> None:
        if self._running:
            return
        # prepare JSONL logger with full primitive data for replay/simulation
        path = Path("run_logs") / f"opportunities_{int(time.time())}.jsonl"
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            self._jsonl_file = open(path, "a", encoding="utf-8")
            log.info("OPP_JSONL path=%s", path)
        except Exception as exc:  # noqa: BLE001
            log.warning("Failed to open opportunity JSONL %s: %s", path, exc)

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
        if self._jsonl_file:
            try:
                self._jsonl_file.flush()
                self._jsonl_file.close()
            except Exception:
                pass
            self._jsonl_file = None

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
        # Keep last run snapshot (all opps, including non-profitable) for UI/debug
        self._last_run_opps = opportunities
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

        # Optional quoter verification (does not gate CSV)
        quoter_row = await self._quoter_enrich(opp)
        # stash quoter data for cycle snapshots
        cyc_label = cycle_name
        if quoter_row:
            if not hasattr(self.scenario_runner, "detector_quoter_cache"):
                self.scenario_runner.detector_quoter_cache = {}
            self.scenario_runner.detector_quoter_cache[cyc_label] = {
                "amount_in": quoter_row.get("amount_in"),
                "amount_out": quoter_row.get("amount_out"),
                "profit": quoter_row.get("profit"),
                "roi": quoter_row.get("roi"),
                "roi_bps": (quoter_row.get("roi") or 0) * 10_000,
                "per_leg": quoter_row.get("per_leg"),
                "error": quoter_row.get("error"),
            }
        elif quoter_row is not None:
            log.warning("QUOTER_ERROR cycle=%s error=%s", cycle_name, quoter_row.get("error"))
        # Persist structured JSON line with all primitives for replay/simulation
        await self._append_op_jsonl(opp, quoter_row)
        if self.executor:
            asyncio.create_task(self.executor.simulate_execution(opp))

    def recent_opportunities(self) -> List[Opportunity]:
        return list(self.recent)

    def _fee_for_pool(self, pool_addr: str) -> str:
        meta = self._pool_meta_by_addr.get(pool_addr.lower())
        return str(getattr(meta, "fee_tier", "?")) if meta else "?"

    async def _append_op_jsonl(self, opp: Opportunity, quoter_row: dict | None) -> None:
        """Append a structured JSON line with full primitives for later replay."""
        if not self._jsonl_file:
            return

        pools = [leg.pool.lower() for leg in opp.legs]
        reader = getattr(self.scenario_runner, "state_reader", None)
        states = await reader.get_all_pool_states(pools) if reader else {}
        tokens = self._tokens or {}

        def _tok(sym: str):
            return tokens.get(sym.upper())

        legs_out = []
        for leg in opp.legs:
            meta = self._pool_meta_by_addr.get(leg.pool.lower())
            state = states.get(leg.pool.lower()) or {}
            tok_in = _tok(leg.token_in)
            tok_out = _tok(leg.token_out)
            legs_out.append(
                {
                    "pool": leg.pool.lower(),
                    "network": meta.network if meta else None,
                    "platform": meta.platform if meta else None,
                    "fee_tier": meta.fee_tier if meta else None,
                    "token_in": leg.token_in.upper(),
                    "token_out": leg.token_out.upper(),
                    "token_in_addr": tok_in.address if tok_in else None,
                    "token_out_addr": tok_out.address if tok_out else None,
                    "token_in_decimals": tok_in.decimals if tok_in else None,
                    "token_out_decimals": tok_out.decimals if tok_out else None,
                    "amount_in": leg.amount_in,
                    "amount_out": leg.amount_out,
                    "price_impact": leg.price_impact,
                    "state": {
                        "sqrt_price_x96": state.get("sqrt_price_x96"),
                        "tick": state.get("tick"),
                        "liquidity": state.get("liquidity") or state.get("liquidity_int"),
                        "block_number": state.get("block_number"),
                        "timestamp_ms": state.get("timestamp_ms"),
                        "last_ws_refresh_ms": state.get("last_ws_refresh_ms"),
                        "last_snapshot_ms": state.get("last_snapshot_ms"),
                    },
                }
            )

        ts_ms = int(time.time() * 1000)
        record = {
            "ts_ms": ts_ms,
            "ts_iso": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(ts_ms / 1000)),
            "cycle": "->".join([leg.token_in for leg in opp.legs] + [opp.legs[0].token_in]) if opp.legs else "unknown",
            "roi": opp.total_roi,
            "profit": opp.profit,
            "profit_usd": opp.profit_usd,
            "notional": opp.notional,
            "size_cap": opp.size_cap,
            "execution_cost": opp.execution_cost,
            "confidence": opp.confidence,
            "legs": legs_out,
            "quoter": quoter_row or {},
            "detector": {
                "min_roi_threshold": self.min_roi,
                "version": "v5",
            },
        }
        try:
            self._jsonl_file.write(json.dumps(record, separators=(",", ":")) + "\n")
            self._jsonl_file.flush()
        except Exception:  # noqa: BLE001
            log.warning("Failed to write JSONL opportunity", exc_info=True)

    def get_recent_opportunities(self, limit: int = 50) -> List[Opportunity]:
        return list(self.recent)[-limit:]

    def get_last_run_opportunities(self, limit: int = 200) -> List[Opportunity]:
        """Return the full last-run opportunity list (may include negative ROI)."""
        return list(self._last_run_opps)[-limit:]

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

    async def _quoter_enrich(self, opp: Opportunity) -> dict:
        """
        Call on-chain quoter for the full path using quoteExactInput.
        Uses configured RPC + optional quoter address (BSC_QUOTER_ADDR/ETH_QUOTER_ADDR).
        Returns dict for logging; does not gate detection.
        """
        try:
            from decimal import Decimal
            from v5.simulator.quoter_client import quote_exact_input, DEFAULT_QUOTER
        except Exception:
            return {}

        if not opp.legs:
            return {}

        first_pool = opp.legs[0].pool.lower()
        meta = self._pool_meta_by_addr.get(first_pool)
        net = getattr(meta, "network", "").lower() if meta else ""

        settings = getattr(self.scenario_runner, "settings", None)
        primary_rpc = None
        quoter_addr = None
        if net == "bsc":
            primary_rpc = getattr(settings, "bsc_rpc_url", None) if settings else None
            quoter_addr = getattr(settings, "bsc_quoter_addr", None) if settings else None
        elif net == "eth":
            primary_rpc = getattr(settings, "ethereum_rpc_url", None) if settings else None
            quoter_addr = getattr(settings, "ethereum_quoter_addr", None) if settings else None
        quoter_addr = quoter_addr or DEFAULT_QUOTER

        rpc_candidates = [primary_rpc] if primary_rpc else []
        if net == "bsc" and not primary_rpc:
            rpc_candidates.extend(
                [
                    "https://bsc-dataseed.binance.org/",
                    "https://bsc-dataseed1.ninicoin.io/",
                    "https://bsc-dataseed1.defibit.io/",
                ]
            )
        if not rpc_candidates:
            return {"error": "no_rpc_url"}

        # amount_in: 90% of size_cap (if present) else initial_amount, capped
        initial_amount = Decimal(str(opp.notional or getattr(self.scenario_runner, "initial_amount", 1)))
        base_amount = Decimal(str(opp.size_cap)) if opp.size_cap is not None else initial_amount
        MAX_QUOTER_AMOUNT = Decimal("100")
        if base_amount <= 0:
            base_amount = Decimal("1")
        base_amount = min(base_amount, MAX_QUOTER_AMOUNT)
        amount_in_decimal = base_amount * Decimal("0.9")

        tokens = self._tokens or {}
        # Build path tokens and fees
        path_tokens = []
        path_fees = []
        for leg in opp.legs:
            tok_in = tokens.get(leg.token_in.upper())
            tok_out = tokens.get(leg.token_out.upper())
            if not tok_in or not tok_out:
                return {"error": "missing_token_meta"}
            if not path_tokens:
                path_tokens.append(tok_in.address)
            path_tokens.append(tok_out.address)
            meta_leg = self._pool_meta_by_addr.get(leg.pool.lower())
            if not meta_leg:
                return {"error": "missing_pool_meta"}
            path_fees.append(int(meta_leg.fee_tier))

        start_dec = tokens[opp.legs[0].token_in.upper()].decimals
        amount_raw = int(amount_in_decimal * (Decimal(10) ** start_dec))

        async with aiohttp.ClientSession() as session:
            last_error = None
            for rpc_url in rpc_candidates:
                multi_quotes = []
                for mult in (1, 10, 100, 1000):
                    amt_raw_scaled = amount_raw * mult
                    res = await quote_exact_input(
                        rpc_url=rpc_url,
                        path_tokens=path_tokens,
                        path_fees=path_fees,
                        amount_in=amt_raw_scaled,
                        quoter=quoter_addr,
                        session=session,
                    )
                    if not res:
                        multi_quotes.append(
                            {
                                "multiplier": mult,
                                "amount_in": float(Decimal(amount_raw) * mult / (Decimal(10) ** start_dec)),
                                "error": "quoter_failed",
                            }
                        )
                        continue
                    amount_out_dec = Decimal(res.amount_out) / (Decimal(10) ** start_dec)
                    amt_in_dec = Decimal(amt_raw_scaled) / (Decimal(10) ** start_dec)
                    profit = float(amount_out_dec - amt_in_dec)
                    roi = profit / float(amt_in_dec) if amt_in_dec else 0.0
                    multi_quotes.append(
                        {
                            "multiplier": mult,
                            "amount_in": float(amt_in_dec),
                            "amount_out": float(amount_out_dec),
                            "profit": profit,
                            "roi": roi,
                        }
                    )
                # pick multiplier 1 as primary if it succeeded
                primary = next((q for q in multi_quotes if q.get("multiplier") == 1 and "error" not in q), None)
                if primary:
                    log.info("QUOTER_SUCCESS rpc=%s quoter=%s profit=%.6f roi=%.6f", rpc_url, quoter_addr, primary["profit"], primary["roi"])
                    return {
                        "amount_in": primary["amount_in"],
                        "amount_out": primary["amount_out"],
                        "profit": primary["profit"],
                        "roi": primary["roi"],
                        "path_tokens": path_tokens,
                        "path_fees": path_fees,
                        "quoter": quoter_addr,
                        "rpc_used": rpc_url,
                        "multi_quotes": multi_quotes,
                    }
                last_error = f"quoter_failed rpc={rpc_url} quoter={quoter_addr}"

        return {"error": last_error or "quoter_failed_all_rpc"}
