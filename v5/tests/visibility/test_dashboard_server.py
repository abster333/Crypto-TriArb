from fastapi.testclient import TestClient
from v5.visibility.dashboard_server import DashboardServer
from v5.common.models import PoolMeta, PoolState
from v5.state_store.state_reader import StateReader
from v5.state_store.hot_cache import HotCache
from v5.state_store.snapshot_store import SnapshotStore
import pytest


class FakeRedis:
    def __init__(self):
        self.store = {}
    async def get(self,k): return self.store.get(k)
    async def mget(self,keys): return [self.store.get(k) for k in keys]
    async def set(self,k,v): self.store[k]=v
    async def aclose(self): return None
    async def scan(self,cursor=0,match=None,count=10):
        import fnmatch
        keys = [k for k in self.store.keys() if (match is None or fnmatch.fnmatch(k, match))]
        return 0, keys


@pytest.mark.asyncio
async def test_dashboard_endpoints():
    hot = HotCache("redis://", client=FakeRedis())
    snap = SnapshotStore("redis://", client=FakeRedis())
    await hot.start(); await snap.start()
    meta = PoolMeta(pool_address="0x" + "1"*40, platform="UNI", token0="A", token1="B", fee_tier=500, network="eth")
    await hot.write_pool_state(PoolState(pool_meta=meta, sqrt_price_x96="1", tick=0, liquidity="1", block_number=1, timestamp_ms=1))
    reader = StateReader(hot, snap)
    class DummySim:
        async def run_once(self):
            return []
    server = DashboardServer(reader, DummySim())
    client = TestClient(server.app)
    assert client.get("/health").status_code == 200
    assert client.get("/pools").status_code == 200


@pytest.mark.asyncio
async def test_dashboard_ws_status():
    hot = HotCache("redis://", client=FakeRedis())
    snap = SnapshotStore("redis://", client=FakeRedis())
    await hot.start(); await snap.start()
    meta = PoolMeta(pool_address="0x" + "2"*40, platform="UNI", token0="A", token1="B", fee_tier=500, network="ETH")
    await hot.write_pool_state(PoolState(
        pool_meta=meta,
        sqrt_price_x96="1",
        tick=0,
        liquidity="1",
        block_number=1,
        timestamp_ms=1,
        last_ws_refresh_ms=123456,
    ))
    reader = StateReader(hot, snap)
    class DummyDet:
        def recent_opportunities(self): return []
        def get_recent_opportunities(self, limit=50): return []
        @property
        def scenario_runner(self): return type("obj",(object,),{"cycles":[["A","B","C"]]})()
    server = DashboardServer(reader, DummyDet())
    client = TestClient(server.app)
    resp = client.get("/ws/status")
    assert resp.status_code == 200
    body = resp.json()
    assert body["networks"][0]["connected"] is True
