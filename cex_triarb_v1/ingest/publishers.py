from __future__ import annotations

import json
import logging
from dataclasses import dataclass, asdict
import asyncio
from typing import Any, Dict, List, Optional, Sequence

try:
    import nats
except ImportError:  # pragma: no cover
    nats = None

try:
    import redis.asyncio as redis  # type: ignore
except ImportError:  # pragma: no cover
    redis = None

log = logging.getLogger(__name__)


@dataclass
class SnapshotEvent:
    exchange: str
    symbol: str
    ts_event: int
    ts_ingest: int
    bids: List[List[float]]
    asks: List[List[float]]
    depth: int
    source: str
    latency_ms: Dict[str, float]


@dataclass
class TradeEvent:
    exchange: str
    symbol: str
    ts_event: int
    ts_ingest: int
    side: str
    price: float
    size: float
    trade_id: str


class NatsPublisher:
    def __init__(self, url: str = "nats://127.0.0.1:4222", snapshot_subject: str = "md.snapshot", trade_subject: str = "md.trade", health_subject: str = "md.health") -> None:
        self.url = url
        self.snapshot_subject = snapshot_subject
        self.trade_subject = trade_subject
        self.health_subject = health_subject
        self._nc = None

    async def start(self) -> None:
        if nats is None:
            log.warning("nats-py not installed; NATS publishing disabled")
            return
        if self._nc:
            return
        self._nc = await connect_nats_with_retry(self.url)
        log.info("Connected to NATS at %s", self.url)

    async def stop(self) -> None:
        if self._nc:
            await self._nc.drain()
            await self._nc.close()
        self._nc = None

    async def publish_snapshot(self, event: SnapshotEvent) -> None:
        if not self._nc:
            return
        await self._nc.publish(self.snapshot_subject, json.dumps(asdict(event)).encode())

    async def publish_trade(self, event: TradeEvent) -> None:
        if not self._nc:
            return
        await self._nc.publish(self.trade_subject, json.dumps(asdict(event)).encode())

    async def publish_health(self, payload: Dict[str, Any]) -> None:
        if not self._nc:
            return
        await self._nc.publish(self.health_subject, json.dumps(payload).encode())

    async def subscribe(self, subject: str, cb) -> None:
        if not self._nc:
            return
        await self._nc.subscribe(subject, cb=cb)


async def connect_nats_with_retry(url: str, attempts: int = 5, backoff: float = 1.5, timeout: float = 3.0):
    """Connect to NATS with simple exponential backoff."""
    if nats is None:
        raise RuntimeError("nats-py not installed")
    last_exc = None
    for i in range(1, attempts + 1):
        try:
            return await nats.connect(url, connect_timeout=timeout)
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            wait = backoff ** i
            log.warning("NATS connect failed (attempt %d/%d): %s; retrying in %.2fs", i, attempts, exc, wait)
            await asyncio.sleep(wait)
    raise last_exc


class RedisHotCache:
    def __init__(self, url: str = "redis://127.0.0.1:6379/0", prefix: str = "md") -> None:
        self.url = url
        self.prefix = prefix.rstrip(":")
        self._client = None

    async def start(self) -> None:
        if redis is None:
            log.warning("redis async client not installed; Redis hot cache disabled")
            return
        if self._client:
            return
        self._client = redis.from_url(self.url, decode_responses=True)
        log.info("Connected to Redis at %s", self.url)

    async def stop(self) -> None:
        if self._client:
            await self._client.aclose()
        self._client = None

    async def clear(self) -> int:
        """Delete keys for this prefix (l1/l5/desired_symbols). Returns keys deleted."""
        if not self._client:
            return 0
        deleted = 0
        patterns = [
            f"{self.prefix}:l1:*",
            f"{self.prefix}:l5:*",
            f"{self.prefix}:desired_symbols",
        ]
        for pattern in patterns:
            async for key in self._client.scan_iter(match=pattern):
                deleted += await self._client.delete(key)
        return deleted

    async def write_l1(self, exchange: str, symbol: str, bid: float, ask: float, mid: float, ts_event: int, ts_ingest: int) -> None:
        if not self._client:
            return
        key = f"{self.prefix}:l1:{exchange}:{symbol}"
        await self._client.hset(
            key,
            mapping={
                "bid": bid,
                "ask": ask,
                "mid": mid,
                "ts_event": ts_event,
                "ts_ingest": ts_ingest,
            },
        )

    async def write_l5(self, exchange: str, symbol: str, bids: Sequence[Sequence[float]], asks: Sequence[Sequence[float]], ts_event: int) -> None:
        if not self._client:
            return
        key = f"{self.prefix}:l5:{exchange}:{symbol}"
        await self._client.hset(
            key,
            mapping={
                "bids": json.dumps(bids),
                "asks": json.dumps(asks),
                "ts_event": ts_event,
            },
        )

    async def save_desired_symbols(self, symbols: Sequence[str], per_exchange: Dict[str, Sequence[str]] | None = None) -> None:
        if not self._client:
            return
        # Union list
        key_union = f"{self.prefix}:desired_symbols"
        await self._client.delete(key_union)
        if symbols:
            await self._client.rpush(key_union, *symbols)
        # Per-exchange lists
        if per_exchange:
            for ex, sym_list in per_exchange.items():
                key_ex = f"{self.prefix}:desired_symbols:{ex.upper()}"
                await self._client.delete(key_ex)
                if sym_list:
                    await self._client.rpush(key_ex, *sym_list)

    async def load_desired_symbols(self) -> tuple[list[str], Dict[str, List[str]]]:
        """
        Returns (union_symbols, per_exchange_map). Empty if redis not available or no entries.
        """
        if not self._client:
            return [], {}
        union: list[str] = []
        per: Dict[str, List[str]] = {}
        # Union
        key_union = f"{self.prefix}:desired_symbols"
        try:
            union = await self._client.lrange(key_union, 0, -1)  # type: ignore[arg-type]
        except Exception:
            union = []
        # Per-exchange
        try:
            async for key in self._client.scan_iter(match=f"{self.prefix}:desired_symbols:*"):
                parts = key.split(":")
                if len(parts) < 3:
                    continue
                ex = parts[-1]
                if ex == "desired_symbols":
                    continue
                vals = await self._client.lrange(key, 0, -1)
                per[ex] = vals
        except Exception:
            pass
        return union, per
