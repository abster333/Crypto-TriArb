import pytest

from v5.state_store.snapshot_store import SnapshotStore
from v5.common.models import PoolState, PoolMeta


class FakeRedis:
    def __init__(self):
        self.store = {}

    async def get(self, key):
        return self.store.get(key)

    async def set(self, key, value):
        self.store[key] = value

    async def mget(self, keys):
        return [self.store.get(k) for k in keys]

    async def delete(self, key):
        self.store.pop(key, None)

    async def scan(self, cursor=0, match=None, count=10):
        keys = list(self.store.keys())
        return 0, keys

    async def aclose(self):
        return None


@pytest.mark.asyncio
async def test_snapshot_store_idempotent_and_version():
    store = SnapshotStore("redis://localhost:6379/0", prefix="testv5", client=FakeRedis())
    meta = PoolMeta(pool_address="0x" + "a"*40, platform="TEST", token0="A", token1="B", fee_tier=500, network="eth")
    state = PoolState(
        pool_meta=meta,
        sqrt_price_x96="1",
        tick=0,
        liquidity="1",
        block_number=1,
        timestamp_ms=1,
    )
    written = await store.write_if_changed(state)
    assert written is True
    state2 = state.model_copy(update={"block_number": 2})
    written_again = await store.write_if_changed(state2)
    assert written_again is True
    payload, version, block = await store.read_with_version(meta.pool_address)
    assert version == 2
    assert block == 2


@pytest.mark.asyncio
async def test_snapshot_store_read_many():
    store = SnapshotStore("redis://localhost:6379/0", prefix="testv5", client=FakeRedis())
    meta = PoolMeta(pool_address="0x" + "b"*40, platform="TEST", token0="A", token1="B", fee_tier=500, network="eth")
    state = PoolState(pool_meta=meta, sqrt_price_x96="1", tick=0, liquidity="1", block_number=1, timestamp_ms=1)
    await store.write_if_changed(state)
    res = await store.read_many([meta.pool_address])
    assert meta.pool_address in res


@pytest.mark.asyncio
async def test_snapshot_cleanup():
    store = SnapshotStore("redis://localhost:6379/0", prefix="testv5", client=FakeRedis())
    meta = PoolMeta(pool_address="0x" + "c"*40, platform="TEST", token0="A", token1="B", fee_tier=500, network="eth")
    state = PoolState(pool_meta=meta, sqrt_price_x96="1", tick=0, liquidity="1", block_number=1, timestamp_ms=1)
    await store.write_if_changed(state)
    deleted = await store.cleanup_older_than(retention_ms=0, now_ms=2_000_000_000_000)
    assert deleted >= 1
