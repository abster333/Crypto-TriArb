import asyncio
import pytest
from decimal import Decimal

from v5.ingest.pool_poller import UniswapV3Poller
from v5.ingest.rpc_server import BatchRpcServer
from v5.common.models import PoolMeta, PoolState, Token, OpportunityLeg, Opportunity
from v5.state_store.hot_cache import HotCache
from v5.state_store.snapshot_store import SnapshotStore
from v5.state_store.state_reader import StateReader
from v5.simulator.scenario_runner import ScenarioRunner


class FakeRpc(BatchRpcServer):
    async def submit_pool_state(self, network: str, address: str, *, block_tag=None):
        # simple deterministic slot0/liquidity
        return {"slot0": "0x" + "1"*64 + "0"*63 + "1", "liquidity": "0x2", "block_number": 1}


class FakeRedis:
    def __init__(self):
        self.store = {}
    async def set(self,k,v): self.store[k]=v
    async def get(self,k): return self.store.get(k)
    async def mget(self, keys): return [self.store.get(k) for k in keys]
    async def delete(self,k): self.store.pop(k,None)
    async def expire(self,*a,**k): return True
    async def ping(self): return True
    async def scan(self,cursor=0,match="*",count=10):
        import fnmatch
        keys = [k for k in self.store.keys() if fnmatch.fnmatch(k, match)]
        return 0, keys


@pytest.mark.asyncio
async def test_full_pipeline_smoke():
    # Setup state stores
    redis = FakeRedis()
    hot = HotCache("redis://", client=redis)
    snaps = SnapshotStore("redis://", client=redis)
    await hot.start(); await snaps.start()
    reader = StateReader(hot, snaps)

    # Tokens and pools forming a simple cycle
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

    # Poller pulls states
    rpc = FakeRpc()
    poller = UniswapV3Poller(rpc, pools)
    states = await poller.poll_once()
    # write to stores
    for st in states:
        await hot.write_pool_state(st)
        await snaps.write_if_changed(st)

    runner = ScenarioRunner(reader, tokens, pools, initial_amount=Decimal("1"), min_roi_threshold=0.0)
    runner.cycles = [["A", "B", "C"]]
    opps = await runner.run_once()
    # In this synthetic setup ROI may be zero; ensure pipeline runs
    assert opps is not None
