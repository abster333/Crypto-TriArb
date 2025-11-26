import pytest
from decimal import Decimal, getcontext

from v5.state_store.hot_cache import HotCache
from v5.state_store.snapshot_store import SnapshotStore
from v5.state_store.state_reader import StateReader
from v5.simulator.scenario_runner import ScenarioRunner
from v5.common.models import Token, PoolMeta, PoolState
from v5.simulator import uniswap_v3_math as mathv3

getcontext().prec = 80


class FakeRedis:
    def __init__(self):
        self.store = {}

    async def set(self, key, value):
        self.store[key] = value

    async def get(self, key):
        return self.store.get(key)

    async def mget(self, keys):
        return [self.store.get(k) for k in keys]

    async def delete(self, key):
        self.store.pop(key, None)

    async def expire(self, key, ttl):
        return True

    async def ping(self):
        return True

    async def scan(self, cursor=0, match="*", count=10):
        import fnmatch
        keys = [k for k in self.store.keys() if fnmatch.fnmatch(k, match)]
        return 0, keys


def _sqrt_price_from_price(price: Decimal) -> str:
    sqrt_price = (price.sqrt()) * mathv3.Q96
    return str(int(sqrt_price))


@pytest.mark.asyncio
async def test_full_pipeline_detects_opportunity():
    """Smoke test: write states -> read via ScenarioRunner -> find profitable cycle."""
    redis = FakeRedis()
    hot = HotCache("redis://local", client=redis)
    snaps = SnapshotStore("redis://local", client=redis)
    await hot.start()
    await snaps.start()
    reader = StateReader(hot, snaps)

    tokens = {
        "A": Token(address="0x" + "1"*40, symbol="A", decimals=18, chain_id=1),
        "B": Token(address="0x" + "2"*40, symbol="B", decimals=18, chain_id=1),
        "C": Token(address="0x" + "3"*40, symbol="C", decimals=18, chain_id=1),
    }

    pools = [
        PoolMeta(pool_address="0x" + "a"*40, platform="UNI", token0="A", token1="B", fee_tier=3000, network="ETH", dec0=18, dec1=18),
        PoolMeta(pool_address="0x" + "b"*40, platform="UNI", token0="B", token1="C", fee_tier=3000, network="ETH", dec0=18, dec1=18),
        PoolMeta(pool_address="0x" + "c"*40, platform="UNI", token0="C", token1="A", fee_tier=3000, network="ETH", dec0=18, dec1=18),
    ]

    # Prices aligned with discovered cycle [B, A, C]:
    # B->A 1:1, A->C ~1.01 (by making C cheaper), C->B 1:1
    liquidity = Decimal(10**18)
    states = [
        PoolState(pool_meta=pools[0], sqrt_price_x96=_sqrt_price_from_price(Decimal("1")), tick=0, liquidity=str(liquidity), block_number=1, timestamp_ms=1),
        PoolState(pool_meta=pools[1], sqrt_price_x96=_sqrt_price_from_price(Decimal("1")), tick=0, liquidity=str(liquidity), block_number=1, timestamp_ms=1),
        PoolState(pool_meta=pools[2], sqrt_price_x96=_sqrt_price_from_price(Decimal("0.99")), tick=0, liquidity=str(liquidity), block_number=1, timestamp_ms=1),
    ]

    for st in states:
        await hot.write_pool_state(st)

    runner = ScenarioRunner(reader, tokens, pools, initial_amount=Decimal("1"), min_roi_threshold=0.0)
    runner.cycles = [["B", "A", "C"]]  # deterministic profitable cycle for the fixture
    opportunities = await runner.run_once()
    assert opportunities, "Expected at least one opportunity"
    assert opportunities[0].total_roi > 0
