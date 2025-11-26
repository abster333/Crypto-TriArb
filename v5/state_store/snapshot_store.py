"""Diff- and version-aware snapshot store."""

from __future__ import annotations

import json
import time
from typing import Any, Dict, Iterable, Tuple

import redis.asyncio as redis

from v5.common import metrics
from v5.common.models import PoolState


class SnapshotStore:
    def __init__(self, redis_url: str, prefix: str = "v5:snap", client: redis.Redis | None = None) -> None:
        self.redis_url = redis_url
        self.prefix = prefix.rstrip(":")
        self._client: redis.Redis | None = client

    async def start(self) -> None:
        if self._client is None:
            self._client = redis.from_url(self.redis_url, encoding="utf-8", decode_responses=True)

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    def _key(self, pool: str) -> str:
        return f"{self.prefix}:pool:{pool.lower()}"

    async def clear_all(self) -> None:
        """Remove all snapshot keys for this prefix."""
        assert self._client
        cursor = 0
        pattern = f"{self.prefix}:pool:*"
        try:
            while True:
                cursor, keys = await self._client.scan(cursor=cursor, match=pattern, count=200)
                if keys:
                    await self._client.delete(*keys)
                if cursor == 0 or cursor == "0":
                    break
        except Exception:  # noqa: BLE001
            metrics.STATE_WRITE_ERRORS.labels(source="snapshot_clear_all").inc()

    async def write_if_changed(self, state: PoolState) -> bool:
        """Store snapshot only when fields change; track version & metadata."""
        assert self._client
        key = self._key(state.pool_meta.pool_address)
        raw = await self._client.get(key)
        payload = state.model_dump()
        now = int(time.time() * 1000)
        version = 1
        first_created = now
        if raw:
            try:
                existing = json.loads(raw)
                if existing.get("payload") == payload:
                    return False
                version = int(existing.get("version", 0)) + 1
                first_created = existing.get("first_created_ms", now)
            except json.JSONDecodeError:
                pass
        wrapper = {
            "payload": payload,
            "version": version,
            "block_number": state.block_number,
            "first_created_ms": first_created,
            "last_updated_ms": now,
        }
        try:
            await self._client.set(key, json.dumps(wrapper))
            metrics.SNAPSHOT_VERSION.labels(pool_address=state.pool_meta.pool_address.lower()).set(version)
            return True
        except Exception:  # noqa: BLE001
            metrics.STATE_WRITE_ERRORS.labels(source="snapshot").inc()
            return False

    async def read(self, pool: str) -> Dict[str, Any]:
        state, _, _ = await self.read_with_version(pool)
        return state

    async def read_with_version(self, pool: str) -> Tuple[Dict[str, Any], int, int]:
        assert self._client
        raw = await self._client.get(self._key(pool))
        if not raw:
            return {}, 0, 0
        try:
            wrapper = json.loads(raw)
        except json.JSONDecodeError:
            metrics.STATE_READ_ERRORS.labels(source="snapshot").inc()
            return {}, 0, 0
        payload = wrapper.get("payload") or {}
        version = int(wrapper.get("version", 0))
        block_num = int(wrapper.get("block_number", 0) or 0)
        return payload, version, block_num

    async def read_many(self, pools: Iterable[str]) -> Dict[str, Dict[str, Any]]:
        assert self._client
        keys = [self._key(p) for p in pools]
        try:
            raws = await self._client.mget(keys)
        except Exception:  # noqa: BLE001
            metrics.STATE_READ_ERRORS.labels(source="snapshot_batch").inc()
            return {p: {} for p in pools}
        out: Dict[str, Dict[str, Any]] = {}
        for pool, raw in zip(pools, raws):
            if not raw:
                out[pool] = {}
                continue
            try:
                wrapper = json.loads(raw)
                out[pool] = wrapper.get("payload") or {}
            except json.JSONDecodeError:
                out[pool] = {}
        return out

    async def cleanup_older_than(self, retention_ms: int, now_ms: int | None = None) -> int:
        """Delete snapshots whose last_updated_ms older than retention threshold."""
        assert self._client
        now = now_ms or int(time.time() * 1000)
        cursor = 0
        deleted = 0
        pattern = f"{self.prefix}:pool:*"
        while True:
            cursor, keys = await self._client.scan(cursor=cursor, match=pattern, count=200)
            for key in keys:
                raw = await self._client.get(key)
                if not raw:
                    continue
                try:
                    wrapper = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                updated = wrapper.get("last_updated_ms", 0)
                if updated and now - updated > retention_ms:
                    await self._client.delete(key)
                    deleted += 1
            if cursor == 0:
                break
        return deleted
