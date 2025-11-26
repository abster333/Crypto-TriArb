"""Unified state reader combining hot cache and snapshots."""

from __future__ import annotations

from typing import Dict, List

from v5.common import metrics
from v5.common.models import PoolState, PoolMeta
from v5.state_store.hot_cache import HotCache
from v5.state_store.snapshot_store import SnapshotStore


class StateReader:
    def __init__(self, hot: HotCache, snapshots: SnapshotStore):
        self.hot = hot
        self.snapshots = snapshots

    async def get_pool_state(self, pool_address: str) -> dict:
        """Try hot cache first, fallback to snapshot."""
        hot = await self.hot.read_pool_state(pool_address)
        if hot:
            return hot
        snap, _, _ = await self.snapshots.read_with_version(pool_address)
        return snap

    async def get_all_pool_states(self, pools: List[str]) -> Dict[str, dict]:
        hot = await self.hot.read_many_pool_states(pools)
        missing = [p for p, payload in hot.items() if not payload]
        if not missing:
            return hot
        snap_missing = await self.snapshots.read_many(missing)
        merged = dict(hot)
        for pool, payload in snap_missing.items():
            merged[pool] = payload
        return merged

    async def get_pools_by_network(self, metas: List[PoolMeta], network: str) -> Dict[str, dict]:
        filtered = [m.pool_address for m in metas if m.network.upper() == network.upper()]
        return await self.get_all_pool_states(filtered)

    async def list_all_pool_addresses(self) -> List[str]:
        """Return all known pool addresses from hot cache keys."""
        client = self.hot._client
        if not client:
            return []
        prefix = f"{self.hot.prefix}:pool:"
        cursor = 0
        addresses: List[str] = []
        while True:
            cursor, keys = await client.scan(cursor=cursor, match=f"{prefix}*", count=200)
            for key in keys:
                addr = key.split(":")[-1]
                addresses.append(addr)
            if cursor == 0 or cursor == "0":
                break
        return addresses
