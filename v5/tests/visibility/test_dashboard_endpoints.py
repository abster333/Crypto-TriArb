import pytest
from fastapi.testclient import TestClient

from v5.visibility.dashboard_server import DashboardServer
from v5.state_store.state_reader import StateReader
from v5.state_store.hot_cache import HotCache
from v5.state_store.snapshot_store import SnapshotStore
from v5.simulator.realtime_detector import RealtimeArbDetector


class FakeRedis:
    def __init__(self):
        self.store = {}
    async def get(self,k): return self.store.get(k)
    async def mget(self,keys): return [self.store.get(k) for k in keys]
    async def set(self,k,v): self.store[k]=v
    async def aclose(self): return None
    async def scan(self,cursor=0,match=None,count=10): return 0,[]


class FakeRunner:
    async def run_once(self):
        return []
    @property
    def cycles(self):
        return [["A","B","C"]]


@pytest.mark.asyncio
async def test_dashboard_cycles_and_recent():
    hot = HotCache("redis://", client=FakeRedis())
    snap = SnapshotStore("redis://", client=FakeRedis())
    await hot.start(); await snap.start()
    runner = FakeRunner()
    detector = RealtimeArbDetector(runner, check_interval_ms=1000)
    server = DashboardServer(StateReader(hot, snap), detector)
    client = TestClient(server.app)
    assert client.get("/cycles").status_code == 200
    assert client.get("/opportunities/recent").status_code == 200
