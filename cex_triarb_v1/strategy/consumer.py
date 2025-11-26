from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Set

import redis.asyncio as redis

import nats
from cex_triarb_v1.ingest.publishers import connect_nats_with_retry

from cex_triarb_v1.persistence.ndjson_logger import NdjsonLogger, log_opportunity
QUOTE_SUFFIXES = ["USDT", "USDC", "USD", "BTC", "ETH", "EUR", "GBP"]

log = logging.getLogger(__name__)


def norm_symbol(symbol: str) -> str:
    """Normalize symbol to match ingest/Redis keys (strip separators, uppercase)."""
    return symbol.replace("/", "").replace("-", "").replace("_", "").upper()


@dataclass
class L1:
    bid: float
    bid_size: float
    ask: float
    ask_size: float
    ts_event: int
    ts_ingest: int
    bids: Optional[List[List[float]]] = None
    asks: Optional[List[List[float]]] = None


@dataclass
class Leg:
    exchange: str
    symbol: str
    side: str  # BUY or SELL


@dataclass
class Cycle:
    legs: List[Leg]
    id: str


def parse_cycles(env_value: str) -> List[Cycle]:
    """
    STRAT_CYCLES format:
      EX:PAIR:BUY,EX:PAIR:BUY,EX:PAIR:SELL;EX2:...
    Example (Coinbase BTCUSD/ETHBTC/ETHUSD):
      COINBASE:BTCUSD:BUY,COINBASE:ETHBTC:BUY,COINBASE:ETHUSD:SELL
    """
    cycles: List[Cycle] = []
    if not env_value:
        return cycles
    for raw_cycle in env_value.split(";"):
        legs_raw = [t.strip() for t in raw_cycle.split(",") if t.strip()]
        legs: List[Leg] = []
        for leg_txt in legs_raw:
            parts = leg_txt.split(":")
            if len(parts) != 3:
                continue
            ex, sym, side = parts
            sym = norm_symbol(sym)
            side_norm = side.upper()
            if side_norm not in ("BUY", "SELL"):
                continue
            legs.append(Leg(exchange=ex.upper(), symbol=sym.upper(), side=side_norm))
        if len(legs) == 3:
            cid = "|".join(f"{l.exchange}:{l.symbol}:{l.side}" for l in legs)
            cycles.append(Cycle(legs=legs, id=cid))
    return cycles


def parse_symbol(symbol: str) -> Optional[Tuple[str, str]]:
    s = symbol.upper()
    for suf in QUOTE_SUFFIXES:
        if s.endswith(suf) and len(s) > len(suf):
            return s[: -len(suf)], suf
    return None


def _fill_buy(amount_quote: float, book: L1, use_depth: bool) -> Optional[Tuple[float, float]]:
    """Return (base_out, quote_spent) using asks ladder."""
    asks = book.asks if use_depth and book.asks else None
    ladder = asks or ([[book.ask, book.ask_size]] if book.ask > 0 else [])
    if not ladder:
        return None
    quote_remaining = amount_quote
    base_out = 0.0
    quote_spent = 0.0
    for level in ladder:
        if len(level) < 2:
            continue
        price, size = float(level[0]), float(level[1])
        if price <= 0 or size <= 0:
            continue
        if quote_remaining == float("inf"):
            take = size
            spend = take * price
        else:
            max_base = quote_remaining / price
            take = min(size, max_base)
            spend = take * price
        base_out += take
        quote_spent += spend
        if quote_remaining != float("inf"):
            quote_remaining -= spend
            if quote_remaining <= 1e-9:
                break
    if base_out <= 0:
        return None
    return base_out, quote_spent


def _fill_sell(amount_base: float, book: L1, use_depth: bool) -> Optional[Tuple[float, float]]:
    """Return (quote_out, base_used) using bids ladder."""
    bids = book.bids if use_depth and book.bids else None
    ladder = bids or ([[book.bid, book.bid_size]] if book.bid > 0 else [])
    if not ladder:
        return None
    base_remaining = amount_base
    quote_out = 0.0
    base_used = 0.0
    for level in ladder:
        if len(level) < 2:
            continue
        price, size = float(level[0]), float(level[1])
        if price <= 0 or size <= 0:
            continue
        take = size if base_remaining == float("inf") else min(size, base_remaining)
        quote_out += take * price
        base_used += take
        if base_remaining != float("inf"):
            base_remaining -= take
            if base_remaining <= 1e-9:
                break
    if base_used <= 0:
        return None
    return quote_out, base_used


def apply_leg(amount: float, leg: Leg, book: L1, use_depth: bool) -> Optional[Tuple[float, float]]:
    """
    Returns (new_amount, quote_used) where new_amount is the balance after executing the leg.
    Uses size caps from the book to limit fill.
    """
    if leg.side == "BUY":
        res = _fill_buy(amount, book, use_depth)
        if res is None:
            return None
        base_out, quote_in = res
        return base_out, quote_in
    else:  # SELL
        res = _fill_sell(amount, book, use_depth)
        if res is None:
            return None
        quote_out, base_in = res
        return quote_out, base_in


class StrategyConsumer:
    def __init__(
        self,
        cycles: List[Cycle],
        nats_url: str = "nats://127.0.0.1:4222",
        snapshot_subject: str = "md.snapshot",
        roi_bps: float = 5.0,
        fees_bps: Dict[str, float] | None = None,
        logger: Optional[NdjsonLogger] = None,
        publish_subject: str = "strategy.opportunity",
        redis_url: str | None = None,
        redis_prefix: str = "md",
        use_depth: bool = False,
        debug_depth: bool = False,
        debug_depth_verbose: bool = False,
        start_notional: float = float("inf"),
        enable_depth_optimization: bool = False,
    ) -> None:
        self.cycles = cycles
        self.nats_url = nats_url
        self.snapshot_subject = snapshot_subject
        self.roi_bps = roi_bps
        self.fees_bps = fees_bps or {}
        self.logger = logger or NdjsonLogger("logs/strategy_opps.ndjson")
        self.publish_subject = publish_subject
        self._l1: Dict[Tuple[str, str], L1] = {}
        self._nc: Optional[nats.NATS] = None
        self._sub = None
        self._active: Dict[str, Tuple[int, int]] = {}  # cycle_id -> (first_seen_ts, last_seen_ts)
        self.redis_url = redis_url
        self.redis_prefix = redis_prefix.rstrip(":")
        self._redis = None
        self.use_depth = use_depth
        self.debug_depth = debug_depth
        self.debug_depth_verbose = debug_depth_verbose
        self.start_notional = start_notional
        self.enable_depth_optimization = enable_depth_optimization

    async def start(self) -> None:
        if self.redis_url:
            await self._hydrate_from_redis()
        self._nc = await connect_nats_with_retry(self.nats_url)
        self._sub = await self._nc.subscribe(self.snapshot_subject, cb=self._on_snapshot)
        log.info("StrategyConsumer subscribed to %s", self.snapshot_subject)

    async def stop(self) -> None:
        if self._sub:
            await self._sub.unsubscribe()
        if self._nc:
            await self._nc.drain()
            await self._nc.close()
        if self._redis:
            await self._redis.aclose()
        self._nc = None
        self._sub = None
        self._redis = None

    async def _on_snapshot(self, msg) -> None:
        try:
            payload = json.loads(msg.data.decode())
        except Exception:
            return
        exchange = payload.get("exchange")
        symbol = norm_symbol(payload.get("symbol", ""))
        bids = payload.get("bids") or []
        asks = payload.get("asks") or []
        if not exchange or not symbol or not bids or not asks:
            return
        bid = bids[0][0]
        bid_size = bids[0][1] if len(bids[0]) > 1 else 0.0
        ask = asks[0][0]
        ask_size = asks[0][1] if len(asks[0]) > 1 else 0.0
        l1 = L1(
            bid=bid,
            bid_size=bid_size,
            ask=ask,
            ask_size=ask_size,
            ts_event=payload.get("ts_event", 0),
            ts_ingest=payload.get("ts_ingest", 0),
            bids=bids if self.use_depth else None,
            asks=asks if self.use_depth else None,
        )
        self._l1[(exchange, symbol)] = l1
        await self._evaluate_cycles(exchange, symbol)

    async def _evaluate_cycles(self, exchange: str, symbol: str) -> None:
        now = int(time.time() * 1000)
        for cycle in self.cycles:
            # quick filter: skip cycles not involving this symbol
            if symbol not in [leg.symbol for leg in cycle.legs]:
                continue
            # only evaluate when all legs have books in cache
            missing = [leg for leg in cycle.legs if (leg.exchange, leg.symbol) not in self._l1]
            if missing:
                if self.debug_depth:
                    log.debug("DBG cycle %s waiting for books: %s", cycle.id, [(m.exchange, m.symbol) for m in missing])
                continue
            result = self._calc_cycle(cycle)
            if result is None:
                continue
            start_quote, end_quote, roi_bps, gross_roi_bps, start_ccy, end_ccy, usd_start, usd_end, leg_debug_net, leg_debug_gross = result
            if roi_bps >= self.roi_bps:
                first, _ = self._active.get(cycle.id, (now, now))
                self._active[cycle.id] = (first, now)
                payload = {
                    "cycle_id": cycle.id,
                    "roi_bps": roi_bps,
                    "gross_roi_bps": gross_roi_bps,
                    "quote_start": start_quote,
                    "quote_start_ccy": start_ccy,
                    "quote_end": end_quote,
                    "quote_end_ccy": end_ccy,
                    "notional_usd_start": usd_start,
                    "notional_usd_end": usd_end,
                    "legs": [leg.__dict__ for leg in cycle.legs],
                    "leg_fills_net": leg_debug_net,
                    "leg_fills_gross": leg_debug_gross,
                    "duration_ms": now - first,
                    "ts_detected": now,
                }
                log.info("OPP %s roi=%.2f bps", cycle.id, roi_bps)
                log_opportunity(self.logger, payload)
                if self._nc:
                    await self._nc.publish(self.publish_subject, json.dumps(payload).encode())

    def _calc_cycle(self, cycle: Cycle) -> Optional[Tuple[float, float, float, float, str, str, float, float, List[dict], List[dict]]]:
        """
        Two-phase evaluation:
        Phase 1: top-of-book ROI to quickly gate cycles (no depth walk).
        Phase 2: (future) depth optimization, only if phase1 ROI passes and flag enabled.
        """
        phase1 = self._calc_cycle_top_of_book(cycle)
        if phase1 is None:
            return None
        start_q, end_q, roi_bps, gross_roi_bps, start_ccy, end_ccy, usd_start, usd_end, leg_debug_net, leg_debug_gross = phase1
        if roi_bps < self.roi_bps:
            return None
        if self.debug_depth and (roi_bps >= self.roi_bps or self.debug_depth_verbose):
            leg_summ = "; ".join(
                f"{d['side']} {d['sym']} px={d['px']:.10f} fill={d['fill_sz']:.8f}/{d['avail_sz']} fee_bps={d['fee_bps']:.2f}"
                for d in leg_debug_net
            )
            log.info(
                "PHASE1_CYCLE %s roi=%.2f bps gross=%.2f start=%.6f%s end=%.6f%s legs=[%s]",
                cycle.id,
                roi_bps,
                gross_roi_bps,
                start_q,
                start_ccy,
                end_q,
                end_ccy,
                leg_summ,
            )
        if self.enable_depth_optimization:
            depth_res = self._calc_cycle_depth_optimized(cycle, phase1)
            if depth_res is not None:
                return depth_res
        return start_q, end_q, roi_bps, gross_roi_bps, start_ccy, end_ccy, usd_start, usd_end, leg_debug_net, leg_debug_gross

    def _calc_cycle_top_of_book(self, cycle: Cycle) -> Optional[Tuple[float, float, float, float, str, str, float, float, List[dict], List[dict]]]:
        """
        Phase 1: Compute ROI using only top-of-book prices (best bid/ask) and size caps.
        Returns detailed tuple matching existing payload fields.
        """
        net = self._walk_top_of_book(cycle, apply_fee=True)
        gross = self._walk_top_of_book(cycle, apply_fee=False)
        if net is None or gross is None:
            return None
        start_q_net, end_q_net, start_ccy_net, end_ccy_net, leg_debug_net = net
        start_q_gross, end_q_gross, _, _, leg_debug_gross = gross
        roi_net = (end_q_net - start_q_net) / start_q_net
        roi_gross = (end_q_gross - start_q_gross) / start_q_gross
        usd_start = self._to_usd(start_q_net, start_ccy_net)
        usd_end = self._to_usd(end_q_net, end_ccy_net)
        return (
            start_q_net,
            end_q_net,
            roi_net * 10_000,
            roi_gross * 10_000,
            start_ccy_net,
            end_ccy_net,
            usd_start,
            usd_end,
            leg_debug_net,
            leg_debug_gross,
        )

    def _walk_top_of_book(self, cycle: Cycle, apply_fee: bool) -> Optional[Tuple[float, float, str, str, List[dict]]]:
        # Start with "infinite" notional unless user overrides; flow will be capped by book sizes.
        amt_local = self.start_notional
        start_q: Optional[float] = None
        start_ccy: Optional[str] = None
        current_ccy: Optional[str] = None
        leg_debug: List[dict] = []
        for leg in cycle.legs:
            book = self._l1.get((leg.exchange, leg.symbol))
            if not book:
                if self.debug_depth:
                    log.info("DBG cycle %s missing book %s/%s", cycle.id, leg.exchange, leg.symbol)
                return None
            fee = self.fees_bps.get(leg.exchange, 0.0) / 10_000.0 if apply_fee else 0.0
            base, quote = parse_symbol(leg.symbol) or (None, None)
            if leg.side == "BUY":
                price = book.ask
                size_avail = book.ask_size if book.ask_size and book.ask_size > 0 else float("inf")
                if price is None or price <= 0 or size_avail <= 0:
                    return None
                max_base = amt_local / price
                take = min(size_avail, max_base)
                if take <= 0:
                    return None
                quote_spent = take * price
                quote_spent *= (1 + fee)
                amt_local = take
                if start_q is None:
                    start_q = quote_spent
                    start_ccy = quote
                current_ccy = base
                leg_debug.append(
                    {"side": "BUY", "sym": leg.symbol, "px": price, "fill_sz": take, "avail_sz": size_avail, "fee_bps": fee * 10_000}
                )
            else:  # SELL
                price = book.bid
                size_avail = book.bid_size if book.bid_size and book.bid_size > 0 else float("inf")
                if price is None or price <= 0 or size_avail <= 0:
                    return None
                take = min(size_avail, amt_local)
                if take <= 0:
                    return None
                quote_out = take * price
                quote_out *= (1 - fee)
                amt_local = quote_out
                if start_q is None:
                    start_q = take
                    start_ccy = base
                current_ccy = quote
                leg_debug.append(
                    {"side": "SELL", "sym": leg.symbol, "px": price, "fill_sz": take, "avail_sz": size_avail, "fee_bps": fee * 10_000}
                )
        if start_q is None or amt_local == float("inf"):
            return None
        return start_q, amt_local, start_ccy or "", current_ccy or "", leg_debug

    def _calc_cycle_depth_optimized(
        self, cycle: Cycle, phase1_result: Tuple[float, float, float, float, str, str, float, float]
    ) -> Optional[Tuple[float, float, float, float, str, str, float, float]]:
        """
        Phase 2 placeholder: depth-aware optimization to find max profitable volume.
        TODO: Walk depth levels per leg, maintain ROI > threshold, return execution plan.
        """
        return None

    async def _hydrate_from_redis(self) -> None:
        try:
            self._redis = redis.from_url(self.redis_url, decode_responses=True)
        except Exception as exc:  # noqa: BLE001
            log.warning("Redis hydrate skipped: %s", exc)
            return
        prefix = self.redis_prefix
        keys = []
        try:
            async for k in self._redis.scan_iter(match=f"{prefix}:l1:*"):
                keys.append(k)
        except Exception as exc:  # noqa: BLE001
            log.warning("Redis scan failed: %s", exc)
            return
        hydrated = 0
        hydrated_l5_only = 0
        seen_pairs: Set[tuple[str, str]] = set()
        for key in keys:
            try:
                parts = key.split(":")
                if len(parts) < 4:
                    continue
                ex = parts[-2]
                sym = norm_symbol(parts[-1])
                seen_pairs.add((ex, sym))
                data = await self._redis.hgetall(key)
                bids = []  # ensure defined even if no L5 entry exists
                asks = []
                bid = float(data.get("bid", 0.0))
                ask = float(data.get("ask", 0.0))
                ts_event = int(float(data.get("ts_event", 0)))
                ts_ingest = int(float(data.get("ts_ingest", 0)))
                # Try to hydrate sizes from L5 if present.
                bid_size = 0.0
                ask_size = 0.0
                l5 = await self._redis.hgetall(f"{prefix}:l5:{ex}:{sym}")
                if l5:
                    import json as _json

                    bids = _json.loads(l5.get("bids", "[]"))
                    asks = _json.loads(l5.get("asks", "[]"))
                    if bids and len(bids[0]) > 1:
                        bid_size = float(bids[0][1])
                    if asks and len(asks[0]) > 1:
                        ask_size = float(asks[0][1])
                if bid <= 0 or ask <= 0:
                    continue
                l1 = L1(
                    bid=bid,
                    bid_size=bid_size,
                    ask=ask,
                    ask_size=ask_size,
                    ts_event=ts_event,
                    ts_ingest=ts_ingest,
                    bids=bids if self.use_depth else None,
                    asks=asks if self.use_depth else None,
                )
                self._l1[(ex, sym)] = l1
                hydrated += 1
            except Exception:
                continue
        # Depth-only preload: if no L1 exists, hydrate from L5 ladders.
        if self.use_depth:
            try:
                async for k in self._redis.scan_iter(match=f"{prefix}:l5:*"):
                    parts = k.split(":")
                    if len(parts) < 4:
                        continue
                    ex = parts[-2]
                    sym = norm_symbol(parts[-1])
                    if (ex, sym) in seen_pairs:
                        continue
                    data = await self._redis.hgetall(k)
                    import json as _json

                    bids = _json.loads(data.get("bids", "[]"))
                    asks = _json.loads(data.get("asks", "[]"))
                    ts_event = int(float(data.get("ts_event", 0)))
                    bid = float(bids[0][0]) if bids else 0.0
                    bid_size = float(bids[0][1]) if bids and len(bids[0]) > 1 else 0.0
                    ask = float(asks[0][0]) if asks else 0.0
                    ask_size = float(asks[0][1]) if asks and len(asks[0]) > 1 else 0.0
                    if bid <= 0 or ask <= 0:
                        continue
                    l1 = L1(
                        bid=bid,
                        bid_size=bid_size,
                        ask=ask,
                        ask_size=ask_size,
                        ts_event=ts_event,
                        ts_ingest=ts_event,
                        bids=bids,
                        asks=asks,
                    )
                    self._l1[(ex, sym)] = l1
                    hydrated_l5_only += 1
            except Exception as exc:  # noqa: BLE001
                log.warning("Redis L5 hydrate skipped: %s", exc)
        log.info(
            "Strategy hydration complete: %d L1 entries, %d depth-only entries (prefix=%s)",
            hydrated,
            hydrated_l5_only,
            prefix,
        )

    def _to_usd(self, amount: float, ccy: str) -> float:
        if amount is None or ccy is None:
            return 0.0
        c = ccy.upper()
        if c in ("USD", "USDT", "USDC"):
            return amount  # assume 1:1 for stablecoins
        # try to find conversion pair cUSD or USDc
        pair1 = f"{c}USD"
        pair2 = f"USD{c}"
        book = self._l1.get(("COINBASE", pair1)) or self._l1.get(("KRAKEN", pair1))
        if book:
            mid = (book.bid + book.ask) / 2.0
            return amount * mid
        book = self._l1.get(("COINBASE", pair2)) or self._l1.get(("KRAKEN", pair2))
        if book:
            mid = (book.bid + book.ask) / 2.0
            if mid > 0:
                return amount / mid
        return 0.0


async def run_strategy(
    cycles: List[Cycle],
    nats_url: str,
    snapshot_subject: str,
    roi_bps: float,
    fees_bps: Dict[str, float] | None = None,
    redis_url: str | None = None,
    redis_prefix: str = "md",
    use_depth: bool = False,
    debug_depth: bool = False,
    start_notional: float = float("inf"),
) -> None:
    consumer = StrategyConsumer(
        cycles=cycles,
        nats_url=nats_url,
        snapshot_subject=snapshot_subject,
        roi_bps=roi_bps,
        fees_bps=fees_bps,
        redis_url=redis_url,
        redis_prefix=redis_prefix,
        use_depth=use_depth,
        debug_depth=debug_depth,
        start_notional=start_notional,
    )
    await consumer.start()
    stop_event = asyncio.Event()

    def _stop(*_args) -> None:
        stop_event.set()

    try:
        import signal

        for sig in (signal.SIGINT, signal.SIGTERM):
            asyncio.get_running_loop().add_signal_handler(sig, _stop)
    except Exception:
        pass

    await stop_event.wait()
    await consumer.stop()
QUOTE_SUFFIXES = ["USDT", "USDC", "USD", "BTC", "ETH", "EUR", "GBP"]
