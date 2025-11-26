import pytest
import asyncio

from v5.state_store.state_reader import StateReader
from v5.state_store.hot_cache import HotCache
from v5.state_store.snapshot_store import SnapshotStore
from v5.common.models import PoolMeta, PoolState


class FakeRedis:
    def __init__(self):
        self.store = {}

    async def get(self, key):
        return self.store.get(key)

    async def mget(self, keys):
        return [self.store.get(k) for k in keys]

    async def set(self, key, value):
        self.store[key] = value

    async def scan(self, cursor=0, match=None, count=10):
        return 0, []

    async def delete(self, key):
        self.store.pop(key, None)


@pytest.mark.asyncio
async def test_state_reader_fallback_to_snapshot():
    hot_client = FakeRedis()
    snap_client = FakeRedis()
    hot = HotCache("redis://", client=hot_client)
    snap = SnapshotStore("redis://", client=snap_client)
    await hot.start(); await snap.start()
    meta = PoolMeta(pool_address="0x" + "3"*40, platform="UNI", token0="A", token1="B", fee_tier=500, network="eth")
    state = PoolState(pool_meta=meta, sqrt_price_x96="0x1", tick=0, liquidity="0x2", block_number=1, timestamp_ms=1)
    await snap.write_if_changed(state)
    reader = StateReader(hot, snap)
    res = await reader.get_pool_state(meta.pool_address)
    assert res["pool_meta"]["pool_address"] == meta.pool_address


@pytest.mark.asyncio
async def test_state_reader_cache_hit():
    hot_client = FakeRedis()
    snap_client = FakeRedis()
    hot = HotCache("redis://", client=hot_client)
    snap = SnapshotStore("redis://", client=snap_client)
    await hot.start(); await snap.start()
    meta = PoolMeta(pool_address="0x" + "4"*40, platform="UNI", token0="A", token1="B", fee_tier=500, network="eth")
    state = PoolState(pool_meta=meta, sqrt_price_x96="0x1", tick=0, liquidity="0x2", block_number=1, timestamp_ms=1)
    await hot.write_pool_state(state)
    reader = StateReader(hot, snap)
    res = await reader.get_pool_state(meta.pool_address)
    assert res["pool_meta"]["pool_address"] == meta.pool_address
