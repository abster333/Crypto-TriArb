import pytest
from decimal import Decimal

from v5.simulator.scenario_runner import ScenarioRunner
from v5.common.models import PoolMeta, Token, PoolState
from v5.state_store.state_reader import StateReader
from v5.state_store.hot_cache import HotCache
from v5.state_store.snapshot_store import SnapshotStore


class FakeRedis:
    def __init__(self):
        self.store = {}
    async def get(self,k): return self.store.get(k)
    async def mget(self,keys): return [self.store.get(k) for k in keys]
    async def set(self,k,v): self.store[k]=v
    async def aclose(self): return None
    async def scan(self,cursor=0,match=None,count=10): return 0,[]


@pytest.mark.asyncio
async def test_scenario_runner_returns_opportunities():
    hot = HotCache("redis://", client=FakeRedis())
    snap = SnapshotStore("redis://", client=FakeRedis())
    await hot.start(); await snap.start()
    tokens = {"A": Token(address="0x"+"1"*40, symbol="A", decimals=18, chain_id=1), "B": Token(address="0x"+"2"*40, symbol="B", decimals=18, chain_id=1), "C": Token(address="0x"+"3"*40, symbol="C", decimals=18, chain_id=1)}
    meta1 = PoolMeta(pool_address="0x"+"a"*40, platform="UNI", token0="A", token1="B", fee_tier=500, network="eth")
    meta2 = PoolMeta(pool_address="0x"+"b"*40, platform="UNI", token0="B", token1="C", fee_tier=500, network="eth")
    meta3 = PoolMeta(pool_address="0x"+"c"*40, platform="UNI", token0="C", token1="A", fee_tier=500, network="eth")
    state = PoolState(pool_meta=meta1, sqrt_price_x96=str(2**96), tick=0, liquidity=str(10**18), block_number=1, timestamp_ms=1)
    for m in (meta1, meta2, meta3):
        await hot.write_pool_state(state.model_copy(update={"pool_meta": m}))
    reader = StateReader(hot, snap)
    runner = ScenarioRunner(reader, tokens, [meta1, meta2, meta3], initial_amount=Decimal("1"), min_roi_threshold=0.0)
    opps = await runner.run_once()
    assert isinstance(opps, list)
