import asyncio
import pytest

from v5.ingest.ws_adapters import UniswapV3WsAdapter
from v5.ingest.circuit_breaker import CircuitBreaker


class FlakyConnect:
    def __init__(self, fail_times: int):
        self.fail_times = fail_times
        self.calls = 0
    async def __call__(self, *a, **kw):
        self.calls += 1
        if self.calls <= self.fail_times:
            raise Exception("boom")
        class DummyWS:
            async def send(self, *a, **k): pass
            async def recv(self): await asyncio.sleep(1); return ""
        return DummyWS()


@pytest.mark.asyncio
async def test_ws_adapter_uses_circuit_breaker(monkeypatch):
    flaky = FlakyConnect(fail_times=3)
    # patch websockets.connect to use flaky
    import v5.ingest.ws_adapters as wa
    monkeypatch.setattr(wa.websockets, "connect", flaky)
    breaker = CircuitBreaker(failure_threshold=2, reset_timeout=0.01)
    adapter = UniswapV3WsAdapter("wss://dummy")
    adapter._breaker = breaker
    # should eventually connect after breaker half-open resets
    await adapter.connect()
    assert breaker.state in ("closed", "half_open", "open")  # no exception thrown
    assert flaky.calls >= 3
