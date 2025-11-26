import pytest

from v5.ingest.pool_poller import decode_slot0, UniswapV3Poller
from v5.common.models import PoolMeta


class FakeRPC:
    def __init__(self, slot0="0x" + "0"*62 + "01" + "0"*60 + "01", liquidity="0x2"):
        self.slot0 = slot0
        self.liq = liquidity

    async def submit_pool_state(self, network, address):
        return {"slot0": self.slot0, "liquidity": self.liq, "block_number": 5}


@pytest.mark.asyncio
async def test_decode_slot0_tick_and_sqrt():
    sqrt_price, tick = decode_slot0("0x" + "1"*64 + "0"*63 + "1")
    assert sqrt_price > 0
    assert tick == 1


@pytest.mark.asyncio
async def test_poller_handles_pool():
    meta = PoolMeta(pool_address="0x" + "1"*40, platform="UNI", token0="A", token1="B", fee_tier=500, network="ETH")
    poller = UniswapV3Poller(FakeRPC(), [meta], poll_interval=5)
    res = await poller.poll_once()
    assert len(res) == 1
    assert res[0].block_number == 5
