"""Redis-backed hot cache for V5 state."""

from __future__ import annotations

import json
import time
from typing import Iterable, List

import redis.asyncio as redis

from v5.common import metrics
from v5.common.models import DepthLevels, PoolState


class HotCache:
    """Thin async wrapper over Redis for hot pool state."""

    def __init__(self, redis_url: str, prefix: str = "v5:hot", ttl_seconds: int | None = None, client: redis.Redis | None = None) -> None:
        self.redis_url = redis_url
        self.prefix = prefix.rstrip(":")
        self.ttl_seconds = ttl_seconds
        self._client: redis.Redis | None = client

    async def start(self) -> None:
        if self._client is None:
            self._client = redis.from_url(self.redis_url, encoding="utf-8", decode_responses=True)

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    def _key(self, kind: str, *parts: str) -> str:
        return ":".join([self.prefix, kind, *parts])

    # --- Writes ---------------------------------------------------------
    async def write_pool_state(self, state: PoolState) -> None:
        """Idempotent write of pool state payload."""
        await self._write_json(self._key("pool", state.pool_meta.pool_address.lower()), state.model_dump(), kind="pool_state")

    async def write_pool_states_batch(self, states: List[PoolState]) -> None:
        """Batch write pool states with a pipeline for efficiency."""
        if not states:
            return
        assert self._client
        pipe = self._client.pipeline()
        if hasattr(pipe, "__await__"):
            pipe = await pipe
        for state in states:
            key = self._key("pool", state.pool_meta.pool_address.lower())
            pipe.set(key, json.dumps(state.model_dump()))
            if self.ttl_seconds:
                pipe.expire(key, self.ttl_seconds)
        try:
            await pipe.execute()
            metrics.STATE_CACHE_HIT.labels(source="write_batch").inc(0)  # touch metric family
        except Exception:  # noqa: BLE001
            metrics.STATE_WRITE_ERRORS.labels(source="batch").inc()

    async def write_depth(self, pool: str, depth: DepthLevels) -> None:
        await self._write_json(self._key("depth", pool.lower()), depth.model_dump(), kind="depth")

    async def _write_json(self, key: str, payload: dict, kind: str) -> None:
        assert self._client
        try:
            await self._client.set(key, json.dumps(payload))
            if self.ttl_seconds:
                await self._client.expire(key, self.ttl_seconds)
            metrics.STATE_CACHE_HIT.labels(source=f"write_{kind}").inc(0)  # ensure metric exists
        except Exception:  # noqa: BLE001
            metrics.STATE_WRITE_ERRORS.labels(source=kind).inc()

    async def clear_pool_state(self, pool: str) -> None:
        """Remove a pool state key if present."""
        assert self._client
        key = self._key("pool", pool.lower())
        try:
            await self._client.delete(key)
        except Exception:  # noqa: BLE001
            metrics.STATE_WRITE_ERRORS.labels(source="clear").inc()

    async def clear_all(self) -> None:
        """Remove all hot cache keys for this prefix (pool/depth)."""
        assert self._client
        cursor = 0
        patterns = [self._key("pool", "*"), self._key("depth", "*")]
        try:
            for pattern in patterns:
                cursor = 0
                while True:
                    cursor, keys = await self._client.scan(cursor=cursor, match=pattern, count=200)
                    if keys:
                        await self._client.delete(*keys)
                    if cursor == 0 or cursor == "0":
                        break
        except Exception:  # noqa: BLE001
            metrics.STATE_WRITE_ERRORS.labels(source="clear_all").inc()

    # --- Reads ----------------------------------------------------------
    async def read_pool_state(self, pool: str) -> dict:
        assert self._client
        return await self._read_json(self._key("pool", pool.lower()), source="single")

    async def read_depth(self, pool: str) -> dict:
        assert self._client
        return await self._read_json(self._key("depth", pool.lower()), source="depth")

    async def read_many_pool_states(self, pools: Iterable[str]) -> dict[str, dict]:
        assert self._client
        keys = [self._key("pool", p.lower()) for p in pools]
        try:
            raw_values = await self._client.mget(keys)
        except Exception:  # noqa: BLE001
            metrics.STATE_READ_ERRORS.labels(source="batch").inc()
            return {p: {} for p in pools}
        output: dict[str, dict] = {}
        now = time.time()
        for pool_id, raw in zip(pools, raw_values):
            payload = self._decode_with_ttl_check(raw, key=self._key("pool", pool_id.lower()), now=now)
            output[pool_id] = payload
            if payload:
                metrics.STATE_CACHE_HIT.labels(source="batch").inc()
            else:
                metrics.STATE_CACHE_MISS.labels(source="batch").inc()
        return output

    async def health(self) -> bool:
        """Ping Redis to verify connectivity."""
        assert self._client
        try:
            pong = await self._client.ping()
            return bool(pong)
        except Exception:  # noqa: BLE001
            metrics.STATE_READ_ERRORS.labels(source="health").inc()
            return False

    async def _read_json(self, key: str, source: str) -> dict:
        try:
            raw = await self._client.get(key) if self._client else None
        except Exception:  # noqa: BLE001
            metrics.STATE_READ_ERRORS.labels(source=source).inc()
            return {}
        payload = self._decode_with_ttl_check(raw, key=key)
        if payload:
            metrics.STATE_CACHE_HIT.labels(source=source).inc()
        else:
            metrics.STATE_CACHE_MISS.labels(source=source).inc()
        return payload

    def _decode_with_ttl_check(self, raw: str | None, key: str, now: float | None = None) -> dict:
        if not raw:
            return {}
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        # TTL check is handled by Redis; for tests with fake clients we can optionally embed _expires_at
        exp = payload.get("_expires_at")
        if exp and (now or time.time()) > exp:
            # stale: best-effort delete
            try:
                if self._client:
                    res = self._client.delete(key)
                    if hasattr(res, "__await__"):
                        # fire and forget best-effort
                        try:
                            import asyncio
                            asyncio.create_task(res)  # pragma: no cover
                        except Exception:
                            pass
            except Exception:  # noqa: BLE001
                pass
            return {}
        return payload
