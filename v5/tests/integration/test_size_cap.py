from decimal import Decimal

from v5.common.models import OpportunityLeg, PoolMeta, PoolState
from v5.simulator.roi_calc import compute_roi


def make_pool(addr: str, token0: str, token1: str, fee: int, sqrt_price_x96: int, liquidity: int, tick: int = 0):
    meta = PoolMeta(
        pool_address=addr,
        platform="PANCAKE",
        token0=token0,
        token1=token1,
        fee_tier=fee,
        network="BSC",
        dec0=18,
        dec1=18,
    )
    return PoolState(
        pool_meta=meta,
        sqrt_price_x96=str(sqrt_price_x96),
        tick=tick,
        liquidity=str(liquidity),
        block_number=1,
        timestamp_ms=0,
    )


def test_size_cap_positive_break_even():
    """
    Build a synthetic profitable triangle with modest price skews and verify size_cap > 0
    and ROI > 0 from compute_roi (no fallback to 0). Also verify the cap is a real
    break-even point: ROI is positive below cap and non-positive above cap.
    """
    # Prices (token order token0/token1):
    # A/B ~ 1.00, B/C ~ 0.95, C/A ~ 1.10 => cycle gain ~ 1.045 before fees
    TWO96 = 2 ** 96
    pool_ab = make_pool("0x" + "1" * 40, "A", "B", 100, int((1.0 ** 0.5) * TWO96), 10**24)
    pool_bc = make_pool("0x" + "2" * 40, "B", "C", 100, int((0.95 ** 0.5) * TWO96), 10**24)
    pool_ca = make_pool("0x" + "3" * 40, "C", "A", 100, int((1.10 ** 0.5) * TWO96), 10**24)

    pool_states = {
        pool_ab.pool_meta.pool_address: pool_ab,
        pool_bc.pool_meta.pool_address: pool_bc,
        pool_ca.pool_meta.pool_address: pool_ca,
    }

    legs = [
        OpportunityLeg(pool=pool_ab.pool_meta.pool_address, token_in="A", token_out="B", amount_in=1.0, amount_out=1.0, price_impact=0.0),
        OpportunityLeg(pool=pool_bc.pool_meta.pool_address, token_in="B", token_out="C", amount_in=1.0, amount_out=1.0, price_impact=0.0),
        OpportunityLeg(pool=pool_ca.pool_meta.pool_address, token_in="C", token_out="A", amount_in=1.0, amount_out=1.0, price_impact=0.0),
    ]

    opp = compute_roi(legs, pool_states, Decimal("10"), min_roi=-1.0)
    assert opp is not None
    assert opp.total_roi > 0
    assert opp.size_cap is not None
    # size_cap should be positive and finite (not defaulted to initial_amount by fallback)
    assert opp.size_cap > 0
    assert opp.size_cap < 1e9

    # Validate break-even behavior around the cap
    cap = Decimal(str(opp.size_cap))
    roi_below = compute_roi(legs, pool_states, cap * Decimal("0.9"), min_roi=-1.0)
    roi_above = compute_roi(legs, pool_states, cap * Decimal("1.1"), min_roi=-1.0)
    assert roi_below is not None and roi_below.total_roi > 0
    assert roi_above is None or roi_above.total_roi <= 0
