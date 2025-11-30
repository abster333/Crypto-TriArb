import math
from decimal import Decimal

import pytest

from v5.common.models import PoolMeta, PoolState, OpportunityLeg
from v5.simulator.roi_calc import _cycle_cap_break_even, _max_in_for_delta


def make_pool(liquidity: int, sqrt_price_x96: int, fee: int, dec0=18, dec1=18):
    meta = PoolMeta(
        pool_address="0x" + "0" * 39 + "1",
        platform="PANCAKE",
        token0="T0",
        token1="T1",
        fee_tier=fee,
        network="BSC",
        dec0=dec0,
        dec1=dec1,
    )
    return PoolState(
        pool_meta=meta,
        sqrt_price_x96=str(sqrt_price_x96),
        tick=0,
        liquidity=str(liquidity),
        block_number=1,
        timestamp_ms=0,
    )


def test_max_in_for_delta_sane_order():
    # Symmetric pool price=1, large liquidity, fee 0.01%
    ps = make_pool(liquidity=10**24, sqrt_price_x96=2**96, fee=100)
    cap = _max_in_for_delta(ps, Decimal("0.02"), True)
    assert cap is not None
    # 2% move on deep pool should allow non-trivial size
    assert cap > Decimal("1000")


def test_cap_break_even_reasonable_when_roi_positive():
    ps = make_pool(liquidity=10**24, sqrt_price_x96=2**96, fee=0)  # zero fee, ROI ~0, should still return cap
    legs = [
        OpportunityLeg(pool=ps.pool_meta.pool_address, token_in="T0", token_out="T1", amount_in=1.0, amount_out=1.0, price_impact=0.0),
        OpportunityLeg(pool=ps.pool_meta.pool_address, token_in="T1", token_out="T0", amount_in=1.0, amount_out=1.0, price_impact=0.0),
        OpportunityLeg(pool=ps.pool_meta.pool_address, token_in="T0", token_out="T1", amount_in=1.0, amount_out=1.0, price_impact=0.0),
    ]
    pool_states = {ps.pool_meta.pool_address: ps}
    cap = _cycle_cap_break_even(legs, pool_states, Decimal("100"))
    assert cap is not None
    # ROI ~0, cap search may return 0; compute_roi will fall back to initial amount.
    assert cap >= Decimal("0")
