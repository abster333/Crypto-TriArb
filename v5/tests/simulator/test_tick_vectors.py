import json
from pathlib import Path
from decimal import Decimal

import pytest

from v5.simulator import uniswap_v3_math as mathv3


FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "slot0_live.json"


def _load_vectors():
    with FIXTURE.open() as fh:
        return json.load(fh)


@pytest.mark.parametrize("vec", _load_vectors())
def test_price_matches_coingecko_quote(vec):
    price = mathv3.price_from_sqrt_price(
        Decimal(vec["sqrt_price_x96"]),
        vec["decimals_token0"],
        vec["decimals_token1"],
    )
    expected = Decimal(str(vec["price_quote"]))
    # allow 10% drift between live chain price and cached coingecko quote
    rel_err = abs(price - expected) / expected
    assert rel_err < Decimal("0.10"), f"price drift {rel_err} too high for {vec['pool_name']}"


@pytest.mark.parametrize("vec", _load_vectors())
def test_tick_roundtrip_exact(vec):
    tick_est = mathv3.tick_from_sqrt_price(Decimal(vec["sqrt_price_x96"]))
    assert tick_est == vec["tick"]


def test_multi_tick_crossing_with_live_sqrt():
    """Use a live sqrt as start; cross two artificial boundaries to ensure multi-limit logic works."""
    vec = _load_vectors()[0]  # WETH/USDT pool
    sqrt_start = Decimal(vec["sqrt_price_x96"])
    liquidity = Decimal(10**9)
    limits = [sqrt_start * Decimal("0.9"), sqrt_start * Decimal("0.8")]
    res = mathv3.simulate_swap(
        sqrt_price_x96=sqrt_start,
        liquidity=liquidity,
        amount_in=Decimal("1e18"),  # ample to cross limits
        zero_for_one=True,
        fee_tier=500,
        decimals_token0=vec["decimals_token0"],
        decimals_token1=vec["decimals_token1"],
        tick_boundaries=limits,
    )
    assert res.sqrt_price_after <= limits[-1] + Decimal("1e-6")
