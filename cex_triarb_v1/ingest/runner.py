from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import time
from typing import Awaitable, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from .ws_coinbase import CoinbaseWsAdapter, CoinbaseTicker
from .ws_kraken import KrakenWsAdapter, KrakenTicker
from .depth import DepthSnapshot
from .publishers import NatsPublisher, RedisHotCache, SnapshotEvent, TradeEvent
from .rest_fallback import rest_refresh
from .health_server import start_health_server
from .publishers import RedisHotCache

log = logging.getLogger(__name__)

TickerHandler = Callable[[str, object], Awaitable[None]]
DepthHandler = Callable[[DepthSnapshot], Awaitable[None]]


class WsIngestManager:
    """
    Thin coordinator that owns exchange adapters and lets callers hot-swap the symbol set.
    - start(): boot adapters for the current symbol list.
    - update_symbols(): restart adapters when the desired set changes.
    - stop(): cancel all running adapters.
    """

    def __init__(
        self,
        symbols: Sequence[str],
        on_ticker: Optional[TickerHandler] = None,
        on_depth: Optional[DepthHandler] = None,
        exchanges: Iterable[str] = ("COINBASE", "KRAKEN"),
        per_exchange: Dict[str, Sequence[str]] | None = None,
        depth_levels: int = 5,
        coinbase_channels: Sequence[str] | None = None,
        kraken_book_depth: int = 10,
        coinbase_auth: tuple[str | None, str | None, str | None] | None = None,
        coinbase_mode: str = "EXCHANGE",
        coinbase_batch_size: int = 40,
        kraken_batch_size: int = 100,
        okx_book_depth: int = 5,
        okx_batch_size: int = 250,
        redis_url: str = "redis://127.0.0.1:6379/0",
        redis_prefix: str = "md",
    ) -> None:
        self._symbols: List[str] = [s.upper() for s in symbols]
        self._on_ticker = on_ticker or self._noop_ticker
        # Preserve None to fully disable depth subscriptions; avoid substituting a noop that would still trigger book subs.
        self._on_depth = on_depth
        self._depth_enabled = on_depth is not None
        self._exchanges = {ex.upper() for ex in exchanges}
        self._per_exchange = {k.upper(): [s.upper() for s in v] for k, v in (per_exchange or {}).items()}
        self._depth_levels = max(1, depth_levels)
        # If no depth handler is provided, force ticker-only to avoid level2 subs.
        if not self._depth_enabled:
            self._coinbase_channels = ["ticker"]
        else:
            self._coinbase_channels = list(coinbase_channels) if coinbase_channels else None
        self._kraken_book_depth = max(self._depth_levels, kraken_book_depth)
        self._kraken_batch_size = max(1, kraken_batch_size)
        self._coinbase_auth = coinbase_auth
        self._coinbase_mode = (coinbase_mode or "EXCHANGE").upper()
        self._coinbase_batch_size = max(1, coinbase_batch_size)
        self._okx_book_depth = max(1, okx_book_depth)
        self._okx_batch_size = max(1, okx_batch_size)
        self._tasks: Dict[str, asyncio.Task] = {}
        self._adapters = {}
        self._lock = asyncio.Lock()
        self._redis_cache = RedisHotCache(url=redis_url, prefix=redis_prefix)

    async def start(self) -> None:
        async with self._lock:
            await self._start_locked()

    async def stop(self) -> None:
        async with self._lock:
            for task in self._tasks.values():
                task.cancel()
            for task in self._tasks.values():
                with contextlib.suppress(asyncio.CancelledError):
                    await task
        self._tasks.clear()
        self._adapters.clear()

    async def _prune_failed_symbol(self, inst_id: str) -> None:
        """Remove a failed instId from desired symbols in Redis so downstream cycles don't wait on it."""
        if not inst_id or not self._redis_cache or not self._redis_cache._client:
            return
        sym = inst_id.replace("-", "").upper()
        try:
            # remove from union list
            prefix = self._redis_cache.prefix
            await self._redis_cache._client.lrem(f"{prefix}:desired_symbols", 0, sym)
            await self._redis_cache._client.lrem(f"{prefix}:desired_symbols:OKX", 0, sym)
            log.warning("Pruned failed OKX symbol %s (instId=%s) from desired symbols", sym, inst_id)
        except Exception as exc:  # noqa: BLE001
            log.warning("Failed to prune symbol %s from Redis: %s", sym, exc)
    async def update_symbols(self, symbols: Sequence[str]) -> None:
        symbols_norm = [s.upper() for s in symbols]
        if set(symbols_norm) == set(self._symbols):
            return
        log.info("Updating symbols: %s -> %s", self._symbols, symbols_norm)
        self._symbols = symbols_norm
        await self.stop()
        await self.start()

    def _symbols_for(self, exchange: str) -> List[str]:
        return self._per_exchange.get(exchange, self._symbols)

    def _chunks(self, items: Sequence[str], size: int) -> List[List[str]]:
        return [list(items[i : i + size]) for i in range(0, len(items), size)]

    async def _start_locked(self) -> None:
        # start redis cache for pruning before adapters come up
        await self._redis_cache.start()
        if "COINBASE" in self._exchanges:
            coinbase_symbols = self._symbols_for("COINBASE")
            batches = [coinbase_symbols]
            if self._coinbase_mode == "ADVANCED" and len(coinbase_symbols) > self._coinbase_batch_size:
                batches = self._chunks(coinbase_symbols, self._coinbase_batch_size)
            for idx, batch in enumerate(batches):
                if self._coinbase_mode == "ADVANCED":
                    from .ws_coinbase_adv import CoinbaseAdvWsAdapter

                    adapter = CoinbaseAdvWsAdapter(
                        batch,
                        on_ticker=lambda t: self._on_ticker("COINBASE", t),
                        on_depth=self._on_depth if self._depth_enabled else None,
                        channels=self._coinbase_channels,
                        depth_levels=self._depth_levels,
                        api_key=self._coinbase_auth[0] if self._coinbase_auth else None,
                        api_secret=self._coinbase_auth[1] if self._coinbase_auth else None,
                    )
                else:
                    adapter = CoinbaseWsAdapter(
                        batch,
                        on_ticker=lambda t: self._on_ticker("COINBASE", t),
                        on_depth=self._on_depth if self._depth_enabled else None,
                        channels=self._coinbase_channels,
                        depth_levels=self._depth_levels,
                        api_key=self._coinbase_auth[0] if self._coinbase_auth else None,
                        api_secret=self._coinbase_auth[1] if self._coinbase_auth else None,
                        api_passphrase=self._coinbase_auth[2] if self._coinbase_auth else None,
                    )
                key = "COINBASE" if len(batches) == 1 else f"COINBASE-{idx}"
                self._adapters[key] = adapter
                self._tasks[key] = asyncio.create_task(adapter.run_forever(), name=f"cb-ws-{idx}")
        if "KRAKEN" in self._exchanges:
            kraken_symbols = self._symbols_for("KRAKEN")
            batches = self._chunks(kraken_symbols, self._kraken_batch_size)
            for idx, batch in enumerate(batches):
                adapter = KrakenWsAdapter(
                    batch,
                    on_ticker=lambda t, ex="KRAKEN": self._on_ticker(ex, t),
                    on_depth=self._on_depth if self._depth_enabled else None,
                    depth_levels=self._depth_levels,
                    book_subscription_depth=self._kraken_book_depth,
                )
                key = "KRAKEN" if len(batches) == 1 else f"KRAKEN-{idx}"
                self._adapters[key] = adapter
                self._tasks[key] = asyncio.create_task(adapter.run_forever(), name=f"kr-ws-{idx}")
        if "OKX" in self._exchanges:
            from .ws_okx import OkxWsAdapter
            okx_symbols = self._symbols_for("OKX")
            batches = self._chunks(okx_symbols, self._okx_batch_size)
            for idx, batch in enumerate(batches):
                adapter = OkxWsAdapter(
                    batch,
                    on_depth=self._on_depth if self._depth_enabled else None,
                    depth_levels=self._okx_book_depth,
                    prune_failed=self._prune_failed_symbol,
                )
                key = "OKX" if len(batches) == 1 else f"OKX-{idx}"
                self._adapters[key] = adapter
                self._tasks[key] = asyncio.create_task(adapter.run_forever(), name=f"okx-ws-{idx}")

    async def _noop_ticker(self, exchange: str, ticker: object) -> None:
        log.debug("TICK %s %s", exchange, ticker)

    async def _noop_depth(self, snapshot: DepthSnapshot) -> None:
        log.debug("DEPTH %s %s %s/%s", snapshot.exchange, snapshot.symbol, len(snapshot.bids), len(snapshot.asks))


__all__ = ["WsIngestManager", "TickerHandler", "DepthHandler"]


class IngestService:
    """High-level orchestrator: websockets + rest fallback + publishers + control-plane."""

    def __init__(
        self,
        symbols: Sequence[str],
        exchanges: Iterable[str] = ("COINBASE", "KRAKEN"),
        nats_url: str = "nats://127.0.0.1:4222",
        redis_url: str = "redis://127.0.0.1:6379/0",
        redis_prefix: str = "md",
        snapshot_subject: str = "md.snapshot",
        health_subject: str = "md.health",
        control_subject: str = "strategy.ctrl.symbols",
        rest_interval_sec: int = 10,
        health_port: int = 8085,
        rest_enabled: bool = False,
        per_exchange_symbols: Dict[str, Sequence[str]] | None = None,
        depth_enabled: bool = False,
        depth_levels: int = 5,
        coinbase_channels: Sequence[str] | None = None,
        kraken_book_depth: int = 10,
        coinbase_ws_mode: str = "EXCHANGE",
        coinbase_api_key: str | None = None,
        coinbase_api_secret: str | None = None,
        coinbase_api_passphrase: str | None = None,
        coinbase_batch_size: int = 40,
        kraken_batch_size: int = 100,
        okx_book_depth: int = 5,
        okx_batch_size: int = 300,
    ) -> None:
        self.symbols = [s.upper() for s in symbols]
        self.exchanges = {ex.upper() for ex in exchanges}
        self.rest_interval_sec = rest_interval_sec
        self.control_subject = control_subject
        self.health_port = health_port
        self.rest_enabled = rest_enabled
        self.per_exchange_symbols = {k.upper(): [s.upper() for s in v] for k, v in (per_exchange_symbols or {}).items()}
        self.redis_prefix = redis_prefix.rstrip(":")
        self.depth_enabled = depth_enabled
        self.depth_levels = max(1, depth_levels)
        # If depth is disabled, force ticker-only to avoid subscribing to L2.
        if self.depth_enabled:
            self.coinbase_channels = list(coinbase_channels) if coinbase_channels else None
            self.kraken_book_depth = max(self.depth_levels, kraken_book_depth)
            self.okx_book_depth = max(self.depth_levels, okx_book_depth)
        else:
            self.coinbase_channels = ["ticker"]
            self.kraken_book_depth = self.depth_levels  # unused when depth is off
            self.okx_book_depth = self.depth_levels
        self.kraken_batch_size = max(1, kraken_batch_size)
        self.coinbase_ws_mode = (coinbase_ws_mode or "EXCHANGE").upper()
        self.coinbase_batch_size = max(1, coinbase_batch_size)
        self.coinbase_api_key = coinbase_api_key
        self.coinbase_api_secret = coinbase_api_secret
        self.coinbase_api_passphrase = coinbase_api_passphrase
        self.redis_url = redis_url
        self.redis_prefix = redis_prefix.rstrip(":")
        self._redis_cache = RedisHotCache(url=self.redis_url, prefix=self.redis_prefix)

        self.nats = NatsPublisher(url=nats_url, snapshot_subject=snapshot_subject, trade_subject="md.trade", health_subject=health_subject)
        self.redis = RedisHotCache(url=redis_url, prefix=self.redis_prefix)
        self.ws_manager = WsIngestManager(
            self.symbols,
            on_ticker=self._handle_ticker,
            on_depth=self._handle_depth if self.depth_enabled else None,
            exchanges=self.exchanges,
            per_exchange=self.per_exchange_symbols,
            depth_levels=self.depth_levels,
            coinbase_channels=self.coinbase_channels,
            kraken_book_depth=self.kraken_book_depth,
            coinbase_mode=self.coinbase_ws_mode,
            coinbase_auth=(self.coinbase_api_key, self.coinbase_api_secret, self.coinbase_api_passphrase),
            coinbase_batch_size=self.coinbase_batch_size,
            kraken_batch_size=self.kraken_batch_size,
            okx_book_depth=self.okx_book_depth,
            okx_batch_size=self.okx_batch_size,
            redis_url=self.redis_url,
            redis_prefix=self.redis_prefix,
        )
        self._health = {"ws_last_tick": {}, "ws_last_depth": {}, "rest_last": 0}
        self._tasks: List[asyncio.Task] = []
        self._health_runner = None
        self._started = False
        self._health_errors: List[str] = []
        self._rest_disabled_reason: str | None = None

    async def start(self) -> None:
        if self._started:
            return
        self._started = True
        await self.nats.start()
        await self.redis.start()
        await self._redis_cache.start()
        cleared = await self.redis.clear()
        # Clear any desired_symbols lists to force a clean repopulate and avoid stale/failed symbols.
        try:
            if self.redis._client:
                await self.redis._client.delete(f"{self.redis_prefix}:desired_symbols")
                async for key in self.redis._client.scan_iter(match=f"{self.redis_prefix}:desired_symbols:*"):
                    await self.redis._client.delete(key)
                # Clear any depth/l1 data under this prefix to avoid stale cache.
                async for key in self.redis._client.scan_iter(match=f"{self.redis_prefix}:l1:*"):
                    await self.redis._client.delete(key)
                async for key in self.redis._client.scan_iter(match=f"{self.redis_prefix}:l5:*"):
                    await self.redis._client.delete(key)
                log.info("Cleared desired_symbols lists (prefix=%s)", self.redis_prefix)
        except Exception as exc:
            log.warning("Failed to clear desired_symbols lists: %s", exc)
        log.info("Redis hot cache cleared %d keys (prefix=%s)", cleared, self.redis_prefix)
        await self.redis.save_desired_symbols(self.symbols, self.per_exchange_symbols)
        await self.ws_manager.start()
        if self.nats._nc:
            await self.nats.subscribe(self.control_subject, self._on_control_message)
        if self.rest_enabled:
            self._tasks.append(asyncio.create_task(self._rest_loop(), name="rest-fallback"))
        else:
            self._rest_disabled_reason = "disabled (config)"
        self._health_runner = await start_health_server(self.health_port, self._health_snapshot)
        log.info("IngestService started with symbols=%s exchanges=%s", self.symbols, self.exchanges)

    async def stop(self) -> None:
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task
        self._tasks.clear()
        await self.ws_manager.stop()
        await self.nats.stop()
        await self.redis.stop()
        if self._health_runner:
            try:
                await self._health_runner.cleanup()  # type: ignore[attr-defined]
            except Exception:
                pass
        self._started = False

    async def _handle_ticker(self, exchange: str, ticker: object) -> None:
        ts_ingest = int(time.time() * 1000)
        if isinstance(ticker, CoinbaseTicker):
            bid = ticker.best_bid if ticker.best_bid > 0 else ticker.price
            ask = ticker.best_ask if ticker.best_ask > 0 else ticker.price
            size = ticker.size
            symbol = ticker.symbol
            ts_event = ticker.ts_event
        elif isinstance(ticker, KrakenTicker):
            bid = ticker.best_bid if ticker.best_bid > 0 else ticker.price
            ask = ticker.best_ask if ticker.best_ask > 0 else ticker.price
            size = ticker.size
            symbol = ticker.symbol
            ts_event = ticker.ts_event
        else:
            log.debug("Unknown ticker type %s", type(ticker))
            return
        mid = (bid + ask) / 2.0
        snapshot = SnapshotEvent(
            exchange=exchange,
            symbol=symbol,
            ts_event=ts_event,
            ts_ingest=ts_ingest,
            bids=[[bid, size, size]],
            asks=[[ask, size, size]],
            depth=1,
            source="ws",
            latency_ms={"ws_ms": max(0, ts_ingest - ts_event)},
        )
        await self.nats.publish_snapshot(snapshot)
        await self.redis.write_l1(exchange, symbol, bid=bid, ask=ask, mid=mid, ts_event=ts_event, ts_ingest=ts_ingest)
        self._health["ws_last_tick"][exchange] = ts_ingest

    async def _handle_depth(self, snapshot: DepthSnapshot) -> None:
        ts_ingest = int(time.time() * 1000)
        bids = [[p, s, c] for p, s, c in snapshot.bids]
        asks = [[p, s, c] for p, s, c in snapshot.asks]
        event = SnapshotEvent(
            exchange=snapshot.exchange,
            symbol=snapshot.symbol,
            ts_event=snapshot.ts_event,
            ts_ingest=ts_ingest,
            bids=bids,
            asks=asks,
            depth=snapshot.depth,
            source="ws_depth",
            latency_ms={"ws_ms": max(0, ts_ingest - snapshot.ts_event)},
        )
        await self.nats.publish_snapshot(event)
        await self.redis.write_l5(snapshot.exchange, snapshot.symbol, bids=bids, asks=asks, ts_event=snapshot.ts_event)
        self._health["ws_last_depth"][snapshot.exchange] = ts_ingest

    async def _rest_loop(self) -> None:
        while True:
            await asyncio.sleep(self.rest_interval_sec)
            if self._rest_resume_at and time.time() < self._rest_resume_at:
                continue
            try:
                prices = await rest_refresh(self.symbols, exchanges=self.exchanges)
                now = int(time.time() * 1000)
                for (exchange, symbol), (price, latency) in prices.items():
                    snapshot = SnapshotEvent(
                        exchange=exchange,
                        symbol=symbol,
                        ts_event=now,
                        ts_ingest=now,
                        bids=[[price, 0.0, 0.0]],
                        asks=[[price, 0.0, 0.0]],
                        depth=1,
                        source="rest",
                        latency_ms={"rest_ms": latency},
                    )
                    await self.nats.publish_snapshot(snapshot)
                    await self.redis.write_l1(exchange, symbol, bid=price, ask=price, mid=price, ts_event=now, ts_ingest=now)
                if prices:
                    self._health["rest_last"] = now
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                log.warning("REST fallback loop error: %s", exc)
                self._health_errors.append(str(exc))

    async def _on_control_message(self, msg) -> None:
        try:
            payload = json.loads(msg.data.decode()) if hasattr(msg, "data") else {}
            symbols = payload.get("symbols") or payload.get("pairs")
            if not symbols:
                return
            await self.ws_manager.update_symbols(symbols)
            await self.redis.save_desired_symbols(symbols)
            self.symbols = [s.upper() for s in symbols]
            log.info("Control-plane updated symbols -> %s", self.symbols)
        except Exception as exc:  # noqa: BLE001
            log.warning("Control-plane message error: %s", exc)

    def _health_snapshot(self) -> Dict:
        return {
            "symbols": self.symbols,
            "exchanges": sorted(self.exchanges),
            "rest_last": self._health.get("rest_last", 0),
            "ws_last_tick": self._health.get("ws_last_tick", {}),
            "ws_last_depth": self._health.get("ws_last_depth", {}),
            "errors": self._health_errors[-5:],
            "rest_disabled_reason": self._rest_disabled_reason,
        }
