from __future__ import annotations

import asyncio
import json
import logging
import time
import hmac
import hashlib
import base64
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Awaitable, Callable, Optional, Sequence

import websockets

from .depth import DepthBook, DepthSnapshot
from .ws_base import BaseWsAdapter
from . import metrics

log = logging.getLogger(__name__)

_DEFAULT_URL = "wss://ws-feed.exchange.coinbase.com"


@dataclass
class CoinbaseTicker:
    product_id: str
    symbol: str
    price: float
    size: float
    side: str
    ts_event: int
    best_bid: float
    best_ask: float
    best_bid_size: float
    best_ask_size: float


def to_product_id(symbol: str) -> str:
    """Convert internal symbol (BTCUSDT/BTC-USD/BTC/USD) -> Coinbase product id."""
    symbol = symbol.strip().upper()
    if "/" in symbol:
        base, quote = symbol.split("/", 1)
    elif symbol.endswith("USD"):
        base, quote = symbol[:-3], symbol[-3:]
    elif symbol.endswith("USDT"):
        base, quote = symbol[:-4], symbol[-4:]
    elif symbol.endswith("USDC"):
        base, quote = symbol[:-4], symbol[-4:]
    elif symbol.endswith("BTC"):
        base, quote = symbol[:-3], symbol[-3:]
    elif symbol.endswith("ETH"):
        base, quote = symbol[:-3], symbol[-3:]
    else:
        raise ValueError(f"Unable to convert {symbol} to Coinbase product id")
    if not base or not quote:
        raise ValueError(f"Invalid symbol {symbol}")
    return f"{base}-{quote}"


def from_product_id(product_id: str) -> str:
    base, _, quote = product_id.partition("-")
    if not base or not quote:
        raise ValueError(f"Invalid product id {product_id}")
    return f"{base}{quote}".upper()


def parse_timestamp(value: Optional[str]) -> int:
    if not value:
        return int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    return int(dt.timestamp() * 1000)


class CoinbaseWsAdapter(BaseWsAdapter):
    """Coinbase WS adapter for ticker + depth feeds."""

    def __init__(
        self,
        symbols: Sequence[str],
        url: str = _DEFAULT_URL,
        session_factory: Callable[..., Awaitable] | None = None,
        on_ticker: Optional[Callable[[CoinbaseTicker], Awaitable[None]]] = None,
        on_depth: Optional[Callable[[DepthSnapshot], Awaitable[None]]] = None,
        channels: Sequence[str] | None = None,
        depth_levels: int = 5,
        api_key: str | None = None,
        api_secret: str | None = None,
        api_passphrase: str | None = None,
    ) -> None:
        super().__init__("COINBASE", symbols)
        self.url = url
        # Increase frame limit to avoid 1009 (message too big) for level2 snapshots.
        self.session_factory = session_factory or (lambda u: websockets.connect(u, max_size=5_000_000))
        self._backoff = 1.0
        self._on_ticker = on_ticker
        self._on_depth = on_depth
        self._depth_levels = max(1, depth_levels)
        self._depth_books: dict[str, DepthBook] = {}
        self._depth_symbols_logged: set[str] = set()
        default_channels = ["ticker"]
        self._channels = list(channels) if channels is not None else default_channels
        self._api_key = api_key
        self._api_secret = api_secret
        self._api_passphrase = api_passphrase

    def build_subscribe_payload(self) -> str:
        products = [to_product_id(symbol) for symbol in self.symbols]
        body = {
            "type": "subscribe",
            "product_ids": products,
            "channels": self._channels,
        }
        if self._needs_auth():
            auth_fields = self._auth_fields()
            if auth_fields is None:
                log.warning("Coinbase level2 requested but API creds not provided; falling back to ticker only")
                body["channels"] = ["ticker"]
            else:
                body.update(auth_fields)
        return json.dumps(body)

    def _needs_auth(self) -> bool:
        return any(ch for ch in self._channels if ch.lower().startswith("level2")) or "full" in self._channels

    def _auth_fields(self) -> dict | None:
        # Coinbase legacy Exchange requires passphrase; Advanced Trade WS uses key+secret only.
        # If passphrase is absent, try key+secret auth (works for Advanced Trade).
        if not (self._api_key and self._api_secret):
            return None
        timestamp = str(time.time())
        message = f"{timestamp}GET/users/self/verify"
        try:
            secret = base64.b64decode(self._api_secret)
        except Exception:
            log.warning("Coinbase API secret is not valid base64; auth skipped")
            return None
        signature = hmac.new(secret, message.encode(), hashlib.sha256).digest()
        sig_b64 = base64.b64encode(signature).decode()
        payload = {"signature": sig_b64, "key": self._api_key, "timestamp": timestamp}
        if self._api_passphrase:
            payload["passphrase"] = self._api_passphrase
        return payload

    async def run_forever(self) -> None:  # pragma: no cover - requires live WS
        attempt = 0
        while True:
            try:
                attempt += 1
                sym_list = ",".join(self.symbols)
                log.info("Coinbase WS connecting (attempt %d) symbols=%s", attempt, sym_list)
                async with self.session_factory(self.url) as ws:
                    await ws.send(self.build_subscribe_payload())
                    self._backoff = 1.0
                    async for message in ws:
                        await self.handle_message(message)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("Coinbase WS loop error (attempt %d): %s", attempt, exc)
                metrics.WS_RECONNECTS.labels(exchange=self.exchange).inc()
                await asyncio.sleep(self._backoff)
                self._backoff = min(self._backoff * 2, 30.0)

    async def handle_message(self, message: str) -> None:
        try:
            payload = json.loads(message)
        except json.JSONDecodeError:
            log.debug("Ignoring non-JSON payload: %s", message)
            metrics.WS_MESSAGE_ERRORS.labels(exchange=self.exchange).inc()
            return
        msg_type = payload.get("type")
        if msg_type == "error":
            log.warning("Coinbase WS error: %s", payload)
            metrics.WS_MESSAGE_ERRORS.labels(exchange=self.exchange).inc()
            return
        if msg_type == "ticker":
            await self._handle_ticker_payload(payload)
        elif msg_type in {"snapshot", "l2update"}:
            await self._handle_depth_payload(payload)

    async def _handle_ticker_payload(self, payload: dict) -> None:
        product_id = payload.get("product_id")
        price = payload.get("price")
        size = payload.get("last_size")
        if not product_id or not price or not size:
            metrics.WS_MESSAGE_ERRORS.labels(exchange=self.exchange).inc()
            return
        try:
            symbol = from_product_id(product_id)
        except ValueError:
            metrics.WS_MESSAGE_ERRORS.labels(exchange=self.exchange).inc()
            return
        best_bid_raw = payload.get("best_bid")
        best_ask_raw = payload.get("best_ask")
        price_f = float(price)
        size_f = float(size)
        best_bid = float(best_bid_raw) if best_bid_raw is not None else price_f
        best_ask = float(best_ask_raw) if best_ask_raw is not None else price_f
        ticker = CoinbaseTicker(
            product_id=product_id,
            symbol=symbol,
            price=price_f,
            size=size_f,
            side=(payload.get("side") or "UNKNOWN").upper(),
            ts_event=parse_timestamp(payload.get("time")),
            best_bid=best_bid,
            best_ask=best_ask,
            best_bid_size=size_f,
            best_ask_size=size_f,
        )
        if self._on_ticker is not None:
            await self._on_ticker(ticker)
        else:
            log.debug("Coinbase ticker %s price=%s size=%s", product_id, price, size)

    async def _handle_depth_payload(self, payload: dict) -> None:
        if self._on_depth is None:
            return
        product_id = payload.get("product_id")
        msg_type = payload.get("type")
        if not product_id or msg_type not in {"snapshot", "l2update"}:
            log.debug("Coinbase depth ignoring payload without product_id/type: %s", payload)
            return
        try:
            symbol = from_product_id(product_id)
        except ValueError:
            metrics.WS_MESSAGE_ERRORS.labels(exchange=self.exchange).inc()
            log.warning("Coinbase depth received unknown product_id=%s", product_id)
            return
        builder = self._depth_books.setdefault(symbol, DepthBook(self.exchange, symbol, self._depth_levels))
        ts_event = parse_timestamp(payload.get("time"))
        if msg_type == "snapshot":
            bids = [(float(price), float(size)) for price, size in payload.get("bids", [])[: self._depth_levels]]
            asks = [(float(price), float(size)) for price, size in payload.get("asks", [])[: self._depth_levels]]
            builder.snapshot(bids, asks, ts_event)
            metrics.WS_DEPTH_RESETS.labels(exchange=self.exchange).inc()
        else:
            changes_raw = payload.get("changes", [])
            changes = []
            for change in changes_raw:
                if len(change) != 3:
                    continue
                side_txt, price_txt, size_txt = change
                side = "bid" if (side_txt or "").lower() == "buy" else "ask"
                try:
                    price_f = float(price_txt)
                    size_f = float(size_txt)
                except (TypeError, ValueError):
                    continue
                changes.append((side, price_f, size_f))
            if not changes:
                log.debug("Coinbase depth update contained no valid changes for %s", symbol)
                return
            builder.update(changes, ts_event)
        snapshot = builder.to_snapshot(source=msg_type)
        self._log_depth_receipt(symbol, msg_type)
        metrics.WS_DEPTH_UPDATES.labels(exchange=self.exchange, kind=msg_type).inc()
        metrics.WS_LAST_DEPTH_TS.labels(exchange=self.exchange, symbol=symbol).set(ts_event)
        await self._on_depth(snapshot)

    def _log_depth_receipt(self, symbol: str, msg_type: str) -> None:
        if symbol in self._depth_symbols_logged:
            return
        log.info("Coinbase depth feed active for %s via %s", symbol, msg_type)
        self._depth_symbols_logged.add(symbol)
