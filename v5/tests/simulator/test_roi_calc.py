from decimal import Decimal
import pytest

from v5.simulator.roi_calc import compute_roi
from v5.common.models import PoolState, PoolMeta, OpportunityLeg


@pytest.mark.asyncio
async def test_profitable_roi():
    meta = PoolMeta(pool_address="0x" + "1"*40, platform="UNI", token0="A", token1="B", fee_tier=500, dec0=18, dec1=18, network="eth")
    state = PoolState(pool_meta=meta, sqrt_price_x96=str(2**96), tick=0, liquidity=str(10**18), block_number=1, timestamp_ms=1)
    legs = [OpportunityLeg(pool=meta.pool_address, token_in="A", token_out="B", amount_in=1.0, amount_out=1.0, price_impact=0)]
    opp = compute_roi(legs, {meta.pool_address: state}, Decimal("1"), min_roi=-1.0)
    assert opp is not None


def test_unprofitable_roi():
    meta = PoolMeta(pool_address="0x" + "2"*40, platform="UNI", token0="A", token1="B", fee_tier=500, dec0=18, dec1=18, network="eth")
    state = PoolState(pool_meta=meta, sqrt_price_x96=str(2**96), tick=0, liquidity=str(0), block_number=1, timestamp_ms=1)
    legs = [OpportunityLeg(pool=meta.pool_address, token_in="A", token_out="B", amount_in=1.0, amount_out=1.0, price_impact=0)]
    opp = compute_roi(legs, {meta.pool_address: state}, Decimal("1"), min_roi=0.0)
    assert opp is None
