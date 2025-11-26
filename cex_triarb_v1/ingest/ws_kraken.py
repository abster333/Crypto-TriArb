from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from typing import Awaitable, Callable, Optional, Sequence

import websockets

from .depth import DepthBook, DepthSnapshot
from .ws_base import BaseWsAdapter
from . import metrics

log = logging.getLogger(__name__)

_DEFAULT_URL = "wss://ws.kraken.com"
_BASE_MAP = {"BTC": "XBT"}
_REVERSE_BASE_MAP = {"XBT": "BTC"}


@dataclass
class KrakenTicker:
    pair: str
    symbol: str
    price: float
    size: float
    ts_event: int
    best_bid: float
    best_ask: float
    best_bid_size: float
    best_ask_size: float


def to_pair(symbol: str) -> str:
    symbol = symbol.strip().upper()
    if symbol.endswith("USD"):
        base, quote = symbol[:-3], symbol[-3:]
    elif symbol.endswith("USDT"):
        base, quote = symbol[:-4], symbol[-4:]
    elif symbol.endswith("USDC"):
        base, quote = symbol[:-4], symbol[-4:]
    elif symbol.endswith("BTC"):
        base, quote = symbol[:-3], symbol[-3:]
    elif symbol.endswith("ETH"):
        base, quote = symbol[:-3], symbol[-3:]
    elif "/" in symbol:
        base, quote = symbol.split("/", 1)
    else:
        raise ValueError(f"Unsupported Kraken symbol {symbol}")
    base = _BASE_MAP.get(base, base)
    return f"{base}/{quote}"


def from_pair(pair: str) -> str:
    pair = pair.upper()
    if "/" in pair:
        base, quote = pair.split("/", 1)
    else:
        base, quote = pair[:3], pair[3:]
    base = _REVERSE_BASE_MAP.get(base, base)
    return f"{base}{quote}"


class KrakenWsAdapter(BaseWsAdapter):
    def __init__(
        self,
        symbols: Sequence[str],
        url: str = _DEFAULT_URL,
        session_factory: Callable[..., Awaitable] | None = None,
        on_ticker: Optional[Callable[[KrakenTicker], Awaitable[None]]] = None,
        on_depth: Optional[Callable[[DepthSnapshot], Awaitable[None]]] = None,
        depth_levels: int = 5,
        book_subscription_depth: int = 10,
    ) -> None:
        super().__init__("KRAKEN", symbols)
        self.url = url
        # Kraken sends heartbeat messages; keep ping_timeout generous to avoid false timeouts.
        self.session_factory = session_factory or (
            lambda u: websockets.connect(u, ping_interval=15, ping_timeout=45, close_timeout=15, max_size=5_000_000)
        )
        self._backoff = 1.0
        self._on_ticker = on_ticker
        self._on_depth = on_depth
        self._depth_levels = max(1, depth_levels)
        self._book_subscription_depth = max(self._depth_levels, book_subscription_depth)
        self._depth_books: dict[str, DepthBook] = {}
        self._depth_symbols_logged: set[str] = set()

    def build_subscribe_payload(self) -> str:
        pairs = [to_pair(symbol) for symbol in self.symbols]
        body = {"event": "subscribe", "pair": pairs, "subscription": {"name": "ticker"}}
        return json.dumps(body)

    def build_book_subscribe_payload(self) -> str:
        pairs = [to_pair(symbol) for symbol in self.symbols]
        body = {
            "event": "subscribe",
            "pair": pairs,
            "subscription": {"name": "book", "depth": self._book_subscription_depth},
        }
        return json.dumps(body)

    async def run_forever(self) -> None:  # pragma: no cover
        attempt = 0
        while True:
            try:
                attempt += 1
                sym_list = ",".join(self.symbols)
                log.info("Kraken WS connecting (attempt %d) symbols=%s", attempt, sym_list)
                async with self.session_factory(self.url) as ws:
                    await ws.send(self.build_subscribe_payload())
                    if self._on_depth is not None:
                        await ws.send(self.build_book_subscribe_payload())
                    self._backoff = 1.0
                    async for message in ws:
                        await self.handle_message(message)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("Kraken WS loop error (attempt %d): %s", attempt, exc)
                metrics.WS_RECONNECTS.labels(exchange=self.exchange).inc()
                await asyncio.sleep(self._backoff)
                self._backoff = min(self._backoff * 1.5, 60.0)

    async def handle_message(self, message: str) -> None:
        try:
            payload = json.loads(message)
        except json.JSONDecodeError:
            metrics.WS_MESSAGE_ERRORS.labels(exchange=self.exchange).inc()
            return
        if isinstance(payload, dict):
            if payload.get("event") == "subscriptionStatus" and payload.get("status") == "error":
                log.warning("Kraken subscription error: %s", payload)
            return
        if not isinstance(payload, list) or len(payload) < 4:
            metrics.WS_MESSAGE_ERRORS.labels(exchange=self.exchange).inc()
            return
        data = payload[1]
        channel = payload[2]
        pair = payload[3]
        if isinstance(channel, str) and channel.startswith("book"):
            await self._handle_book_payload(data, pair, channel)
            return
        if channel != "ticker" or not isinstance(data, dict):
            metrics.WS_MESSAGE_ERRORS.labels(exchange=self.exchange).inc()
            return
        price_entry = data.get("c") or []
        volume_entry = data.get("v") or []
        bid_entry = data.get("b") or []
        ask_entry = data.get("a") or []
        try:
            price = float(price_entry[0]) if price_entry else None
            size = float(volume_entry[1] if len(volume_entry) > 1 else volume_entry[0]) if volume_entry else None
            best_bid = float(bid_entry[0]) if bid_entry else price
            best_ask = float(ask_entry[0]) if ask_entry else price
            best_bid_size = float(bid_entry[1] if len(bid_entry) > 1 else size) if bid_entry else size
            best_ask_size = float(ask_entry[1] if len(ask_entry) > 1 else size) if ask_entry else size
        except (ValueError, TypeError):
            metrics.WS_MESSAGE_ERRORS.labels(exchange=self.exchange).inc()
            return
        if price is None or size is None:
            metrics.WS_MESSAGE_ERRORS.labels(exchange=self.exchange).inc()
            return
        try:
            symbol = from_pair(pair)
        except ValueError:
            metrics.WS_MESSAGE_ERRORS.labels(exchange=self.exchange).inc()
            log.warning("Kraken depth received unknown pair=%s", pair)
            return
        ticker = KrakenTicker(
            pair=pair,
            symbol=symbol,
            price=price,
            size=size,
            ts_event=int(time.time() * 1000),
            best_bid=best_bid,
            best_ask=best_ask,
            best_bid_size=best_bid_size,
            best_ask_size=best_ask_size,
        )
        if self._on_ticker:
            await self._on_ticker(ticker)
        else:
            log.debug("Kraken ticker %s price=%s", pair, price)

    async def _handle_book_payload(self, data, pair: str, channel: str) -> None:
        if self._on_depth is None or not isinstance(data, dict):
            return
        symbol = from_pair(pair)
        builder = self._depth_books.setdefault(symbol, DepthBook(self.exchange, symbol, depth=self._depth_levels))
        ts_event = self._extract_ts(data)
        if "as" in data or "bs" in data:
            bids = [(float(price), float(size)) for price, size, *_ in data.get("bs", [])]
            asks = [(float(price), float(size)) for price, size, *_ in data.get("as", [])]
            builder.snapshot(bids=bids, asks=asks, ts_event=ts_event)
            metrics.WS_DEPTH_RESETS.labels(exchange=self.exchange).inc()
        else:
            changes = []
            for side_key, entries in (("bid", data.get("b")), ("ask", data.get("a"))):
                if not entries:
                    continue
                for price, size, *_ in entries:
                    try:
                        price_f = float(price)
                        size_f = float(size)
                    except (TypeError, ValueError):
                        continue
                    changes.append((side_key, price_f, size_f))
            if not changes:
                log.debug("Kraken depth update contained no valid changes for %s", symbol)
                return
            builder.update(changes, ts_event)
        snapshot = builder.to_snapshot(source=channel)
        if symbol not in self._depth_symbols_logged:
            log.info("Kraken depth feed active for %s via %s", symbol, channel)
            self._depth_symbols_logged.add(symbol)
        metrics.WS_DEPTH_UPDATES.labels(exchange=self.exchange, kind=channel).inc()
        metrics.WS_LAST_DEPTH_TS.labels(exchange=self.exchange, symbol=symbol).set(ts_event)
        await self._on_depth(snapshot)

    @staticmethod
    def _extract_ts(data: dict) -> int:
        for key in ("a", "b", "as", "bs"):
            entries = data.get(key) or []
            for entry in entries:
                if len(entry) >= 3:
                    try:
                        return int(float(entry[2]) * 1000)
                    except (TypeError, ValueError):
                        continue
        return int(time.time() * 1000)
