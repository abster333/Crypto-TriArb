import asyncio
import pytest

from v5.ingest.ws_runner import WsRunner
from v5.ingest.ws_types import WsEvent
from v5.common import metrics
from v5.common.models import PoolMeta


class DummyAdapter:
    def __init__(self, events):
        self.events = asyncio.Queue()
        for e in events:
            self.events.put_nowait(e)
        self.connected = False
    async def connect(self): self.connected = True
    async def subscribe(self, pools): return None
    async def recv_event(self):
        try:
            return await asyncio.wait_for(self.events.get(), timeout=0.1)
        except asyncio.TimeoutError:
            await asyncio.sleep(0.01)
            return None


class DummyMetric:
    def __init__(self): self.values=[]
    def labels(self, **kwargs): self.kwargs=kwargs; return self
    def set(self, v): self.values.append(v)
    def inc(self, val=1): return None


@pytest.mark.asyncio
async def test_ws_runner_backpressure_drops_when_full(monkeypatch):
    # patch metrics
    backpressure = DummyMetric()
    errors = DummyMetric()
    monkeypatch.setattr(metrics, "INGEST_WS_BACKPRESSURE", backpressure)
    monkeypatch.setattr(metrics, "INGEST_ERRORS", errors)

    pool = PoolMeta(pool_address="0x" + "1"*40, platform="UNI", token0="A", token1="B", fee_tier=500, network="ETH")
    queue = asyncio.Queue()
    adapter = DummyAdapter([WsEvent(pool=pool.pool_address, block_number=1, ts_ms=0) for _ in range(5)])
    runner = WsRunner(rpc=None, pools=[pool], queue=queue, adapters=[adapter])
    runner._event_queue = asyncio.Queue(maxsize=1)
    async def _fake_fetch_state(meta): return None
    runner._fetch_state = _fake_fetch_state
    await runner.start()
    await asyncio.sleep(0.2)
    await runner.stop()
    # queue should not exceed maxsize 1; drops recorded
    assert max(backpressure.values) <= 1
