import asyncio
import time
import pytest

from v5.state_store.hot_cache import HotCache
from v5.common.models import PoolMeta, PoolState


class FakeRedis:
    def __init__(self):
        self.store = {}

    def pipeline(self):
        return self.Pipe(self)

    class Pipe:
        def __init__(self, outer):
            self.outer = outer

        def set(self, key, value):
            self.outer.store[key] = value
            return self

        def expire(self, key, ttl):
            import json, time
            payload = self.outer.store.get(key)
            if payload:
                obj = json.loads(payload)
                obj["_expires_at"] = time.time() + ttl
                self.outer.store[key] = json.dumps(obj)
            return self

        async def execute(self):
            return True

    # pipeline compatibility
    async def set(self, key, value):
        self.store[key] = value

    async def expire(self, key, ttl):
        payload = self.store.get(key)
        if payload:
            import json
            obj = json.loads(payload)
            obj["_expires_at"] = time.time() + ttl
            self.store[key] = json.dumps(obj)

    async def get(self, key):
        return self.store.get(key)

    async def mget(self, keys):
        return [self.store.get(k) for k in keys]

    async def delete(self, key):
        self.store.pop(key, None)

    async def ping(self):
        return True


@pytest.mark.asyncio
async def test_batch_write_and_read():
    client = FakeRedis()
    cache = HotCache("redis://localhost", client=client, ttl_seconds=1)
    await cache.start()
    meta = PoolMeta(pool_address="0x" + "1"*40, platform="UNI", token0="A", token1="B", fee_tier=500, network="eth")
    state = PoolState(pool_meta=meta, sqrt_price_x96="0x1", tick=0, liquidity="0x2", block_number=1, timestamp_ms=1)
    await cache.write_pool_states_batch([state])
    res = await cache.read_pool_state(meta.pool_address)
    assert res["pool_meta"]["pool_address"] == meta.pool_address


@pytest.mark.asyncio
async def test_ttl_expires():
    client = FakeRedis()
    cache = HotCache("redis://localhost", client=client, ttl_seconds=0)
    await cache.start()
    meta = PoolMeta(pool_address="0x" + "2"*40, platform="UNI", token0="A", token1="B", fee_tier=500, network="eth")
    state = PoolState(pool_meta=meta, sqrt_price_x96="0x1", tick=0, liquidity="0x2", block_number=1, timestamp_ms=1)
    await cache.write_pool_state(state)
    res = await cache.read_pool_state(meta.pool_address)
    assert res
    # simulate expiry
    key = list(client.store.keys())[0]
    import json
    obj = json.loads(client.store[key])
    obj["_expires_at"] = time.time() - 1
    client.store[key] = json.dumps(obj)
    res2 = await cache.read_pool_state(meta.pool_address)
    assert res2 == {}


@pytest.mark.asyncio
async def test_health_check():
    client = FakeRedis()
    cache = HotCache("redis://localhost", client=client)
    await cache.start()
    assert await cache.health() is True
