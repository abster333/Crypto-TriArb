from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Dict, List, Sequence

from ..persistence.ndjson_logger import NdjsonLogger, log_opportunity

log = logging.getLogger(__name__)


@dataclass
class OrderBookView:
    bid: float
    ask: float
    ts_event: int


@dataclass
class TriCycle:
    exchange: str
    legs: Sequence[str]  # ["BTCUSD", "ETHUSD", "ETHBTC"]
    id: str


class TriArbRunner:
    def __init__(self, cycles: Sequence[TriCycle], roi_bps: float = 5.0, start_notional: float = 1000.0, logger: NdjsonLogger | None = None) -> None:
        self.cycles = list(cycles)
        self.roi_bps = roi_bps
        self.start_notional = start_notional
        self.logger = logger or NdjsonLogger()
        self.books: Dict[tuple[str, str], OrderBookView] = {}
        self._stop = asyncio.Event()

    def update_book(self, exchange: str, symbol: str, bid: float, ask: float, ts_event: int) -> None:
        self.books[(exchange, symbol)] = OrderBookView(bid=bid, ask=ask, ts_event=ts_event)

    async def run(self) -> None:
        while not self._stop.is_set():
            await asyncio.sleep(0.1)

    def stop(self) -> None:
        self._stop.set()

    def evaluate_cycle(self, cycle: TriCycle) -> None:
        try:
            leg1, leg2, leg3 = cycle.legs
            b1 = self.books.get((cycle.exchange, leg1))
            b2 = self.books.get((cycle.exchange, leg2))
            b3 = self.books.get((cycle.exchange, leg3))
            if not all([b1, b2, b3]):
                return
            age_ms = max(int(time.time() * 1000) - min(b1.ts_event, b2.ts_event, b3.ts_event), 0)
            if age_ms > 1000:
                return
            # naive top-of-book calc (buy, sell, sell pattern example)
            amount_usd = self.start_notional
            eth_bought = amount_usd / b1.ask
            btc_bought = eth_bought * b2.bid  # selling ETH for BTC
            usd_final = btc_bought * b3.bid
            roi = (usd_final - amount_usd) / amount_usd
            roi_bps = roi * 10_000
            if roi_bps >= self.roi_bps:
                payload = {
                    "cycle_id": cycle.id,
                    "exchange": cycle.exchange,
                    "roi_bps": roi_bps,
                    "usd_start": amount_usd,
                    "usd_end": usd_final,
                    "ts_detected": int(time.time() * 1000),
                    "age_ms": age_ms,
                }
                log.info("OPPORTUNITY %s roi=%.2f bps", cycle.id, roi_bps)
                log_opportunity(self.logger, payload)
        except Exception as exc:  # noqa: BLE001
            log.warning("cycle evaluation failed: %s", exc)
