from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Awaitable, Callable, Optional, Sequence

import websockets

from .depth import DepthBook, DepthSnapshot
from .ws_base import BaseWsAdapter
from . import metrics

log = logging.getLogger(__name__)

_DEFAULT_URL = "wss://ws.okx.com:8443/ws/v5/public"


def _to_inst_id(symbol: str) -> str:
    """Convert internal symbol (e.g., BTCUSDT) to OKX instId (BTC-USDT)."""
    s = symbol.upper().replace("/", "").replace("-", "")
    # heuristic: quote can be 3-4 chars from whitelist; OKX uses dash separator anyway
    if len(s) < 6:
        return s
    # assume last 4 if endswith USDT/USDC; else last 3
    if s.endswith("USDT") or s.endswith("USDC"):
        return f"{s[:-4]}-{s[-4:]}"
    return f"{s[:-3]}-{s[-3:]}"


def _from_inst_id(inst_id: str) -> str:
    return inst_id.replace("-", "").upper()


class OkxWsAdapter(BaseWsAdapter):
    """
    OKX public websocket adapter.
    Subscribes to books5 (top of book) and tickers for provided symbols.
    """

    def __init__(
        self,
        symbols: Sequence[str],
        url: str = _DEFAULT_URL,
        session_factory: Callable[..., Awaitable] | None = None,
        on_depth: Optional[Callable[[DepthSnapshot], Awaitable[None]]] = None,
        depth_levels: int = 5,
    ) -> None:
        super().__init__("OKX", symbols)
        self.url = url
        self.session_factory = session_factory or (
            lambda u: websockets.connect(u, ping_interval=15, ping_timeout=30, close_timeout=10, max_size=5_000_000)
        )
        self._on_depth = on_depth
        self._depth_books: dict[str, DepthBook] = {}
        self._depth_levels = max(1, depth_levels)
        self._backoff = 1.0

    def _subscribe_payload(self) -> str:
        args = []
        for sym in self.symbols:
            inst = _to_inst_id(sym)
            args.append({"channel": "books5", "instId": inst})
        return json.dumps({"op": "subscribe", "args": args})

    async def run_forever(self) -> None:  # pragma: no cover
        attempt = 0
        while True:
            try:
                attempt += 1
                async with self.session_factory(self.url) as ws:
                    await ws.send(self._subscribe_payload())
                    self._backoff = 1.0
                    async for msg in ws:
                        await self.handle_message(msg)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                log.warning("OKX ws error: %s (attempt=%d)", exc, attempt)
                await asyncio.sleep(min(self._backoff, 30))
                self._backoff = min(self._backoff * 1.5, 30)

    async def handle_message(self, raw: str) -> None:
        try:
            msg = json.loads(raw)
        except Exception:
            metrics.WS_MESSAGE_ERRORS.labels(exchange="OKX").inc()
            return
        if msg.get("event") == "subscribe":
            return
        if "arg" not in msg or "data" not in msg:
            return
        channel = msg["arg"].get("channel")
        inst_id = msg["arg"].get("instId")
        if channel != "books5" or not inst_id:
            return
        for entry in msg.get("data", []) or []:
            await self._handle_book(inst_id, entry)

    async def _handle_book(self, inst_id: str, entry: dict) -> None:
        if self._on_depth is None:
            return
        symbol = _from_inst_id(inst_id)
        book = self._depth_books.setdefault(symbol, DepthBook("OKX", symbol, depth=self._depth_levels))
        bids = entry.get("bids") or []
        asks = entry.get("asks") or []
        ts = int(entry.get("ts") or time.time() * 1000)
        # OKX books5 sends full top book each message; treat as snapshot
        book.snapshot([(float(p), float(sz)) for p, sz, *_ in bids], [(float(p), float(sz)) for p, sz, *_ in asks], ts_event=ts)
        snap = book.to_snapshot(source="ws")
        metrics.WS_DEPTH_UPDATES.labels(exchange="OKX", kind="l2").inc()
        metrics.WS_LAST_DEPTH_TS.labels(exchange="OKX", symbol=symbol).set(ts / 1000.0)
        await self._on_depth(snap)
