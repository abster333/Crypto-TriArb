from decimal import Decimal

import pytest

from v5.simulator import uniswap_v3_math as mathv3


def test_price_and_tick_roundtrip():
    sqrt = Decimal(2) ** 96
    price = mathv3.price_from_sqrt_price(sqrt, 18, 18)
    assert abs(price - Decimal(1)) < Decimal("1e-12")
    tick = mathv3.tick_from_sqrt_price(sqrt)
    assert tick == 0


@pytest.mark.parametrize("tick", [-887272, -200000, -50000, 0, 50000, 200000, 887272])
def test_tick_math_roundtrip_extremes(tick):
    sqrt = mathv3.get_sqrt_ratio_at_tick(tick)
    back = mathv3.tick_from_sqrt_price(Decimal(sqrt))
    assert back == tick


def test_tick_walk_with_liquidity_nets():
    # starting at tick 0, liquidity 1000; add +500 at tick 10, remove -200 at tick -10
    start_sqrt = mathv3.get_sqrt_ratio_at_tick(0)
    liquidity = Decimal(1000)
    nets = [(-10, -200), (10, 500)]
    # trade price up (one_for_zero) should add +500 when crossing tick 10
    res_up = mathv3.simulate_swap(
        sqrt_price_x96=Decimal(start_sqrt),
        liquidity=liquidity,
        amount_in=Decimal(100000),
        zero_for_one=False,
        fee_tier=500,
        decimals_token0=18,
        decimals_token1=18,
        tick_liquidity=list(nets),
    )
    assert res_up.sqrt_price_after > start_sqrt
    # trade price down (zero_for_one) should remove -200 when crossing -10
    res_down = mathv3.simulate_swap(
        sqrt_price_x96=Decimal(start_sqrt),
        liquidity=liquidity,
        amount_in=Decimal(100000),
        zero_for_one=True,
        fee_tier=500,
        decimals_token0=18,
        decimals_token1=18,
        tick_liquidity=list(nets),
    )
    assert res_down.sqrt_price_after < start_sqrt


def test_simulate_swap_basic_zero_for_one():
    sqrt = Decimal(2) ** 96  # price 1
    liquidity = Decimal(10**9)
    amount_in = Decimal(10**6)  # 1e6 units token0
    result = mathv3.simulate_swap(
        sqrt_price_x96=sqrt,
        liquidity=liquidity,
        amount_in=amount_in,
        zero_for_one=True,
        fee_tier=3000,
        decimals_token0=18,
        decimals_token1=18,
    )
    assert result.amount_out > 0
    assert result.sqrt_price_after < sqrt
    # known analytic expected from formula
    fee_fraction = Decimal(3000) / Decimal(1_000_000)
    net = amount_in * (1 - fee_fraction)
    expected_sqrt_next = (liquidity * mathv3.Q96 * sqrt) / ((net * sqrt) + (liquidity * mathv3.Q96))
    assert abs(result.sqrt_price_after - expected_sqrt_next) < Decimal("1e-12") * expected_sqrt_next


def test_simulate_swap_basic_one_for_zero():
    sqrt = Decimal(2) ** 96
    liquidity = Decimal(10**6)
    amount_in = Decimal(10**6)
    result = mathv3.simulate_swap(
        sqrt_price_x96=sqrt,
        liquidity=liquidity,
        amount_in=amount_in,
        zero_for_one=False,
        fee_tier=3000,
        decimals_token0=18,
        decimals_token1=18,
    )
    assert result.amount_out > 0
    assert result.sqrt_price_after > sqrt


def test_zero_liquidity_returns_zero():
    result = mathv3.simulate_swap(
        sqrt_price_x96=Decimal(2) ** 96,
        liquidity=Decimal(0),
        amount_in=Decimal(1000),
        zero_for_one=True,
        fee_tier=3000,
        decimals_token0=18,
        decimals_token1=18,
    )
    assert result.amount_out == 0


def test_extreme_price():
    sqrt = Decimal(2) ** 96 * Decimal(10)
    result = mathv3.simulate_swap(
        sqrt_price_x96=sqrt,
        liquidity=Decimal(10**18),
        amount_in=Decimal(10**6),
        zero_for_one=True,
        fee_tier=3000,
        decimals_token0=18,
        decimals_token1=18,
    )
    assert result.sqrt_price_after > 0


def test_tick_crossing_toward_limit():
    sqrt = Decimal(2) ** 96  # tick 0
    liquidity = Decimal(10**6)
    # set limit to half price => sqrt scaled by 1/sqrt(2)
    sqrt_limit = sqrt / Decimal(2).sqrt()
    amount_in = Decimal(10**12)  # large to hit limit
    result = mathv3.simulate_swap(
        sqrt_price_x96=sqrt,
        liquidity=liquidity,
        amount_in=amount_in,
        zero_for_one=True,
        fee_tier=3000,
        decimals_token0=18,
        decimals_token1=18,
        sqrt_price_limit_x96=sqrt_limit,
    )
    assert result.sqrt_price_after <= sqrt_limit + Decimal("1e-6")


def test_tick_crossing_known_vector_matches_math():
    """Validate amount_out when driving price to a lower limit."""
    sqrt = Decimal(2) ** 96  # price 1
    liquidity = Decimal("1000000")
    sqrt_limit = sqrt / Decimal(2)  # drive price down by 50%
    fee = 3000
    result = mathv3.simulate_swap(
        sqrt_price_x96=sqrt,
        liquidity=liquidity,
        amount_in=Decimal("2000000"),  # more than enough to hit the limit
        zero_for_one=True,
        fee_tier=fee,
        decimals_token0=18,
        decimals_token1=18,
        sqrt_price_limit_x96=sqrt_limit,
    )
    # Expected gross required to hit limit
    fee_fraction = Decimal(fee) / Decimal(1_000_000)
    one_minus_fee = Decimal(1) - fee_fraction
    net_needed = liquidity * (sqrt - sqrt_limit) * mathv3.Q96 / (sqrt_limit * sqrt)
    gross_needed = net_needed / one_minus_fee
    expected_out = liquidity * (sqrt - sqrt_limit) / mathv3.Q96
    assert abs(result.amount_out - expected_out) < Decimal("1e-6")
    assert abs(result.amount_in - Decimal("2000000")) < Decimal("1e-9")
    assert float(result.sqrt_price_after) == pytest.approx(float(sqrt_limit), rel=0, abs=1e-12)


def test_multi_tick_crossing_two_limits():
    sqrt = Decimal(2) ** 96  # start price 1
    liquidity = Decimal("1000000")
    limits = [sqrt / Decimal(2).sqrt(), sqrt / Decimal(4)]  # cross two steps down
    amount_in = Decimal("5000000")  # ample to reach both
    result = mathv3.simulate_swap(
        sqrt_price_x96=sqrt,
        liquidity=liquidity,
        amount_in=amount_in,
        zero_for_one=True,
        fee_tier=3000,
        decimals_token0=18,
        decimals_token1=18,
        tick_boundaries=limits,
    )
    # expected cumulative out = sum over each boundary
    fee_fraction = Decimal(3000) / Decimal(1_000_000)
    one_minus_fee = Decimal(1) - fee_fraction

    def out_between(s_cur, s_next):
        # net in needed (no fee) to reach s_next
        net_in = liquidity * (s_cur - s_next) * mathv3.Q96 / (s_cur * s_next)
        gross_in = net_in / one_minus_fee
        amt_out = liquidity * (s_cur - s_next) / mathv3.Q96
        return gross_in, amt_out

    g1, out1 = out_between(sqrt, limits[0])
    g2, out2 = out_between(limits[0], limits[1])
    expected_total = float(out1 + out2)
    assert float(result.amount_out) == pytest.approx(expected_total, rel=1e-6)
    assert float(result.sqrt_price_after) == pytest.approx(float(limits[1]), rel=0, abs=1e-9)
