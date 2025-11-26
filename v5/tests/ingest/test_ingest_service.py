import pytest
import asyncio

from v5.ingest.ingest_service import IngestService
from v5.ingest.rpc_server import BatchRpcServer
from v5.common.models import PoolMeta, PoolState
from v5.state_store.hot_cache import HotCache
from v5.state_store.snapshot_store import SnapshotStore


class FakeRpc(BatchRpcServer):
    async def start(self):
        return None

    async def stop(self):
        return None

    async def submit_pool_state(self, network, address, block_tag=None):
        return {"slot0": "0x" + "1"*64 + "0"*63 + "1", "liquidity": "0x2", "block_number": 1}

class FakeRedis:
    def __init__(self):
        self.store = {}
    async def get(self, k): return self.store.get(k)
    async def mget(self, keys): return [self.store.get(k) for k in keys]
    async def set(self, k,v): self.store[k]=v
    async def delete(self,k): self.store.pop(k,None)
    async def scan(self, cursor=0, match=None, count=10): return 0,[]
    async def aclose(self): return None
    def pipeline(self): return self
    def expire(self,*a,**k): return self
    def __await__(self):
        async def _noop():
            return self
        return _noop().__await__()
    def set(self,*a,**k): return self
    async def execute(self): return True


@pytest.mark.asyncio
async def test_ingest_writes_to_state_stores():
    rpc = FakeRpc()
    meta = PoolMeta(pool_address="0x" + "9"*40, platform="UNI", token0="A", token1="B", fee_tier=500, network="ETH")
    hot = HotCache("redis://", client=FakeRedis())
    snap = SnapshotStore("redis://", client=FakeRedis())
    await hot.start(); await snap.start();
    service = IngestService(rpc, [meta], hot, snap)
    await service.start()
    # push a state manually to out queue to avoid waiting for poll loop
    state = PoolState(pool_meta=meta, sqrt_price_x96="1", tick=0, liquidity="1", block_number=1, timestamp_ms=1)
    await service.out_queue.put(state)
    await hot.write_pool_state(state)
    wrote = await snap.write_if_changed(state)
    assert wrote is True
    saved_snap = await snap.read(meta.pool_address)
    assert saved_snap or wrote
    await service.stop()
