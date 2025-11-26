import asyncio
import pytest

from v5.ingest.ws_runner import WsRunner
from v5.common.models import PoolMeta


@pytest.mark.asyncio
async def test_ws_runner_buffers_events():
    class FakeRPC:
        async def submit_pool_state(self, _network, _address):
            return {"slot0": "0x" + "1"*64 + "0"*63 + "1", "liquidity": "0x2"}
    rpc = FakeRPC()
    meta = PoolMeta(pool_address="0x" + "5"*40, platform="UNIV3", token0="A", token1="B", fee_tier=500, network="eth")
    queue = asyncio.Queue()
    runner = WsRunner(rpc, [meta], queue, adapters=[])
    await runner.start()
    await runner.handle_raw_swap(meta.pool_address, 1)
    await asyncio.sleep(0)
    await runner.stop()
    assert not queue.empty()
