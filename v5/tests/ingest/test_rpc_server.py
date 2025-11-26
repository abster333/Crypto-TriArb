import asyncio
import pytest

from v5.ingest.rpc_server import BatchRpcServer
from v5.common import metrics


class DummyBreaker:
    async def call(self, fn, *a, **kw):
        return await fn(*a, **kw)


class FakeResponse:
    def __init__(self, payload):
        self._payload = payload
    async def json(self):
        return self._payload
    async def __aenter__(self):
        return self
    async def __aexit__(self, exc_type, exc, tb):
        return False


class FakeSession:
    def __init__(self, payload):
        self.payload = payload
        self.post_calls = 0
        self.last_body = None
    def post(self, url, json):
        self.post_calls += 1
        self.last_body = json
        return FakeResponse(self.payload)


@pytest.mark.asyncio
async def test_pool_batch_includes_block_number_and_maps_results(monkeypatch):
    # dummy metrics to avoid double registration
    class DummyMetric:
        def __init__(self): self.calls=[]; self.values=[]
        def labels(self, **kwargs): self.calls.append(kwargs); return self
        def inc(self, val=1): return None
        def observe(self, v): self.values.append(v); return None
        def set(self, v): return None
    dummy_counter = DummyMetric()
    dummy_hist = DummyMetric()
    monkeypatch.setattr(metrics, "INGEST_RPC_CALLS", dummy_counter)
    monkeypatch.setattr(metrics, "RPC_LATENCY_SECONDS", dummy_hist)

    # prepare fake responses: 1 block + 2* slot/liquidity
    responses = [
        {"id": 1, "result": "0xabc"},  # slot0 addr1
        {"id": 2, "result": "0xdef"},  # liq addr1
        {"id": 3, "result": "0x10"},   # blockNumber
        {"id": 4, "result": "0xabc"},  # slot0 addr2
        {"id": 5, "result": "0xdef"},  # liq addr2
    ]
    session = FakeSession(responses)
    rpc = BatchRpcServer()
    rpc.settings.ethereum_rpc_url = "http://fake"
    rpc._rpc_url_for_network = lambda net: "http://fake"
    rpc._session = session
    rpc._breaker = DummyBreaker()

    # build two pool jobs
    loop = asyncio.get_running_loop()
    fut1 = loop.create_future()
    fut2 = loop.create_future()
    batch = [
        ("pool_state", "eth", "0xaddr1", None, fut1),
        ("pool_state", "eth", "0xaddr2", None, fut2),
    ]
    await rpc._process_batch(batch)
    assert session.post_calls == 1
    # ensure blockNumber call present
    methods = [c["method"] for c in session.last_body]
    assert "eth_blockNumber" in methods
    res1 = fut1.result()
    res2 = fut2.result()
    assert res1["block_number"] == 16
    assert res2["block_number"] == 16
    assert res1["slot0"] == "0xabc"
    assert res1["liquidity"] == "0xdef"
    assert dummy_hist.values, "latency histogram not observed"


@pytest.mark.asyncio
async def test_retry_on_failure(monkeypatch):
    class DummyMetric:
        def labels(self, **kwargs): return self
        def inc(self, val=1): return None
        def observe(self, v): return None
    monkeypatch.setattr(metrics, "INGEST_ERRORS", DummyMetric())
    monkeypatch.setattr(metrics, "RPC_LATENCY_SECONDS", DummyMetric())
    monkeypatch.setattr(metrics, "INGEST_RPC_CALLS", DummyMetric())

    # first call raises, second succeeds
    responses = [{"id": 1, "result": "0x10"}]
    class FlakySession(FakeSession):
        def __init__(self, payload):
            super().__init__(payload)
            self.fail_once = True
        def post(self, url, json):
            if self.fail_once:
                self.fail_once = False
                raise Exception("boom")
            return super().post(url, json)

    session = FlakySession(responses)
    rpc = BatchRpcServer(max_retries=2)
    rpc.settings.ethereum_rpc_url = "http://fake"
    rpc._rpc_url_for_network = lambda net: "http://fake"
    rpc._session = session
    rpc._breaker = DummyBreaker()
    loop = asyncio.get_running_loop()
    fut = loop.create_future()
    batch = [("call", "eth", "eth_blockNumber", [], fut)]
    await rpc._process_batch(batch)
    assert fut.result() == "0x10"
    assert session.post_calls == 1
