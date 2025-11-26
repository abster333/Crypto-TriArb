"""Lightweight Uniswap v3 math (single-range, no tick crossing).

Implements the core SwapMath for a single liquidity range using Decimal
precision. This avoids V4 dependencies and keeps behavior deterministic for
the simulator.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from decimal import Decimal, getcontext

getcontext().prec = 80

Q96 = Decimal(2) ** 96
ONE_0001 = Decimal("1.0001")
MIN_TICK = -887272
MAX_TICK = 887272
MIN_SQRT_RATIO = 4295128739  # from Uniswap TickMath
MAX_SQRT_RATIO = 1461446703485210103287273052203988822378723970342  # inclusive bound in solidity is (ratio at max tick)


@dataclass
class SwapResult:
    amount_in: Decimal
    amount_out: Decimal
    fee_paid: Decimal
    sqrt_price_after: Decimal
    tick_after: int


def price_from_sqrt_price(sqrt_price_x96: Decimal, decimals_token0: int, decimals_token1: int) -> Decimal:
    """Convert sqrtPriceX96 to token0/token1 price."""
    sqrt_dec = Decimal(sqrt_price_x96) / Q96
    scale = Decimal(10) ** (decimals_token0 - decimals_token1)
    return (sqrt_dec * sqrt_dec) * scale


def tick_from_sqrt_price(sqrt_price_x96: Decimal) -> int:
    """Exact-ish inverse of TickMath using binary search (no floats)."""
    if sqrt_price_x96 <= 0:
        raise ValueError("sqrt_price_x96 must be positive")
    sqrt_int = int(Decimal(sqrt_price_x96).to_integral_value(rounding="ROUND_FLOOR"))
    if sqrt_int < MIN_SQRT_RATIO or sqrt_int > MAX_SQRT_RATIO:
        raise ValueError("sqrt_price_x96 out of bounds")
    # Binary search over tick range
    lo, hi = MIN_TICK, MAX_TICK
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if get_sqrt_ratio_at_tick(mid) <= sqrt_int:
            lo = mid
        else:
            hi = mid - 1
    return lo


def get_sqrt_ratio_at_tick(tick: int) -> int:
    """Port of Uniswap V3 TickMath getSqrtRatioAtTick (integer exact)."""
    if tick < MIN_TICK or tick > MAX_TICK:
        raise ValueError("tick out of bounds")
    abs_tick = tick if tick >= 0 else -tick
    # Precomputed ratios from TickMath (as ints)
    ratios = [
        0xfffcb933bd6fad37aa2d162d1a594001,
        0xfff97272373d413259a46990580e213a,
        0xfff2e50f5f656932ef12357cf3c7fdcc,
        0xffe5caca7e10e4e61c3624eaa0941cd0,
        0xffcb9843d60f6159c9db58835c926644,
        0xff973b41fa98c081472e6896dfb254c0,
        0xff2ea16466c96a3843ec78b326b52861,
        0xfe5dee046a99a2a811c461f1969c3053,
        0xfcbe86c7900a88aedcffc83b479aa3a4,
        0xf987a7253ac413176f2b074cf7815e54,
        0xf3392b0822b70005940c7a398e4b70f3,
        0xe7159475a2c29b7443b29c7fa6e889d9,
        0xd097f3bdfd2022b8845ad8f792aa5825,
        0xa9f746462d870fdf8a65dc1f90e061e5,
        0x70d869a156d2a1b890bb3df62baf32f7,
        0x31be135f97d08fd981231505542fcfa6,
        0x9aa508b5b7a84e1c677de54f3e99bc9,
        0x5d6af8dedb81196699c329225ee604,
        0x2216e584f5fa1ea926041bedfe98,
        0x48a170391f7dc42444e8fa2,
    ]
    ratio = 0x100000000000000000000000000000000
    for i in range(20):
        if (abs_tick >> i) & 1:
            ratio = (ratio * ratios[i]) >> 128
    if tick > 0:
        ratio = (1 << 256) // ratio
    # shift to Q96
    # add rounding
    sqrt_ratio_x96 = (ratio >> 32) + (1 if ratio % (1 << 32) else 0)
    return int(sqrt_ratio_x96)


def _simulate_single(
    sqrt_price_x96: Decimal,
    liquidity: Decimal,
    amount_in: Decimal,
    zero_for_one: bool,
    fee_tier: int,
) -> tuple[Decimal, Decimal]:
    """Return (amount_out, sqrt_price_next) for a single-range swap (no crossing)."""
    if liquidity <= 0 or amount_in <= 0:
        return Decimal(0), sqrt_price_x96
    fee_fraction = Decimal(fee_tier) / Decimal(1_000_000)
    net_in = amount_in * (Decimal(1) - fee_fraction)
    sqrt_cur = Decimal(sqrt_price_x96)
    L = Decimal(liquidity)
    if zero_for_one:
        # token0 -> token1
        denominator = (net_in * sqrt_cur) + (L * Q96)
        if denominator == 0:
            return Decimal(0), sqrt_cur
        sqrt_next = (L * Q96 * sqrt_cur) / denominator
        amount_out = L * (sqrt_cur - sqrt_next) / Q96
    else:
        # token1 -> token0
        sqrt_next = sqrt_cur + (net_in * Q96) / L
        amount_out = (L * (sqrt_next - sqrt_cur) * Q96) / (sqrt_next * sqrt_cur)
    return amount_out, sqrt_next


def _max_input_to_price(
    sqrt_price_x96: Decimal,
    liquidity: Decimal,
    target_sqrt: Decimal,
    zero_for_one: bool,
    fee_tier: int,
) -> Decimal:
    """Gross input needed to reach target_sqrt (no fee applied yet)."""
    fee_fraction = Decimal(fee_tier) / Decimal(1_000_000)
    one_minus_fee = Decimal(1) - fee_fraction
    if one_minus_fee <= 0:
        return Decimal(0)
    L = Decimal(liquidity)
    sqrt_cur = Decimal(sqrt_price_x96)
    if zero_for_one:
        if target_sqrt >= sqrt_cur:
            return Decimal(0)
        net = L * (sqrt_cur - target_sqrt) * Q96 / (sqrt_cur * target_sqrt)
    else:
        if target_sqrt <= sqrt_cur:
            return Decimal(0)
        net = L * (target_sqrt - sqrt_cur) / Q96
    return net / one_minus_fee


def simulate_swap(
    sqrt_price_x96: Decimal,
    liquidity: Decimal,
    amount_in: Decimal,
    zero_for_one: bool,
    fee_tier: int,
    decimals_token0: int,
    decimals_token1: int,
    sqrt_price_limit_x96: Decimal | None = None,
    tick_boundaries: list[Decimal] | None = None,
    tick_liquidity: list[tuple[int, int]] | None = None,
) -> SwapResult:
    """Simulate a swap with optional multi-tick crossing toward successive price limits.

    tick_boundaries: ordered list of target sqrt prices to cross (monotonic in trade direction).
    tick_liquidity: list of (tick, liquidity_net) applied when crossing the tick in the
    positive tick direction (price increasing). When moving price down (zero_for_one),
    the liquidity_net is subtracted.
    When amount_in is sufficient to reach the next boundary, we advance and continue; otherwise we
    stop within the current range. This is still a simplification (liquidity is assumed constant).
    """
    remaining_in = Decimal(amount_in)
    total_out = Decimal(0)
    cur_sqrt = Decimal(sqrt_price_x96)
    fee_fraction = Decimal(fee_tier) / Decimal(1_000_000)
    one_minus_fee = Decimal(1) - fee_fraction

    # Build ordered limits in trade direction
    limits: list[Decimal] = []
    if tick_boundaries:
        limits.extend([Decimal(x) for x in tick_boundaries])
    if sqrt_price_limit_x96:
        limits.append(Decimal(sqrt_price_limit_x96))
    # ensure monotonic order
    if zero_for_one:
        limits = [l for l in sorted(set(limits), reverse=True) if l < cur_sqrt]
    else:
        limits = [l for l in sorted(set(limits)) if l > cur_sqrt]

    tick_liq = sorted(tick_liquidity or [], key=lambda t: t[0])

    while remaining_in > 0:
        target = limits[0] if limits else None
        target_from_limits = bool(limits)
        # incorporate tick_liquidity boundaries as dynamic limits
        if tick_liq:
            # compute next boundary in direction
            cur_tick = tick_from_sqrt_price(cur_sqrt)
            if zero_for_one:
                candidates = [t for t in tick_liq if t[0] < cur_tick]
                if candidates:
                    t_boundary = max(candidates, key=lambda x: x[0])
                    boundary_sqrt = Decimal(get_sqrt_ratio_at_tick(t_boundary[0]))
                    if not target or boundary_sqrt > target:
                        target = boundary_sqrt
            else:
                candidates = [t for t in tick_liq if t[0] > cur_tick]
                if candidates:
                    t_boundary = min(candidates, key=lambda x: x[0])
                    boundary_sqrt = Decimal(get_sqrt_ratio_at_tick(t_boundary[0]))
                    if not target or boundary_sqrt < target:
                        target = boundary_sqrt

        if target:
            gross_needed = _max_input_to_price(cur_sqrt, liquidity, target, zero_for_one, fee_tier)
            if gross_needed > 0 and remaining_in >= gross_needed:
                amt_out, next_sqrt = _simulate_single(cur_sqrt, liquidity, gross_needed, zero_for_one, fee_tier)
                total_out += amt_out
                remaining_in -= gross_needed
                cur_sqrt = next_sqrt
                if target_from_limits and limits:
                    limits.pop(0)
                # If this boundary was the final explicit limit, stop
                if target_from_limits and not limits and (sqrt_price_limit_x96 or tick_boundaries):
                    break
                # adjust liquidity if crossing a tick boundary
                if tick_liq:
                    cur_tick = tick_from_sqrt_price(cur_sqrt)
                    for t, liq_net in list(tick_liq):
                        boundary_sqrt = Decimal(get_sqrt_ratio_at_tick(t))
                        crossed = (zero_for_one and cur_sqrt <= boundary_sqrt) or (not zero_for_one and cur_sqrt >= boundary_sqrt)
                        if crossed:
                            tick_liq.remove((t, liq_net))
                            if zero_for_one:
                                liquidity -= Decimal(liq_net)
                            else:
                                liquidity += Decimal(liq_net)
                            if liquidity <= 0:
                                return SwapResult(amount_in=amount_in - remaining_in, amount_out=total_out, fee_paid=amount_in * fee_fraction, sqrt_price_after=cur_sqrt, tick_after=cur_tick)
            continue
        amt_out, next_sqrt = _simulate_single(cur_sqrt, liquidity, remaining_in, zero_for_one, fee_tier)
        total_out += amt_out
        remaining_in = Decimal(0)
        cur_sqrt = next_sqrt
    fee_paid = amount_in - (amount_in * one_minus_fee)
    tick_after = tick_from_sqrt_price(cur_sqrt) if cur_sqrt > 0 else 0
    return SwapResult(
        amount_in=amount_in,
        amount_out=total_out,
        fee_paid=fee_paid,
        sqrt_price_after=cur_sqrt,
        tick_after=tick_after,
    )


__all__ = [
    "SwapResult",
    "simulate_swap",
    "price_from_sqrt_price",
    "tick_from_sqrt_price",
    "Q96",
]
