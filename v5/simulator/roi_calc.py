"""ROI calculation with guardrails."""

from __future__ import annotations

from decimal import Decimal
from typing import Dict, List

import logging

from v5.common.models import Opportunity, OpportunityLeg, PoolState
from v5.simulator import uniswap_v3_math as uni

EPS = Decimal("1e-18")
# Loosen guardrails temporarily to inspect raw outputs
MAX_REALISTIC_ROI = Decimal("1000")
MIN_REALISTIC_ROI = Decimal("-1000")

log = logging.getLogger(__name__)


def compute_roi(legs: List[OpportunityLeg], pool_states: Dict[str, PoolState], initial_amount: Decimal, min_roi: float = 0.0) -> Opportunity | None:
    """Simulate swaps along legs; return Opportunity if profitable."""
    if not legs:
        return None

    def _legs_summary() -> str:
        parts = []
        for leg in legs:
            meta = pool_states.get(leg.pool)
            fee = getattr(meta.pool_meta, "fee_tier", None) if meta else None
            fee_str = f"{fee}" if fee is not None else "?"
            parts.append(f"{leg.token_in}->{leg.token_out}@{leg.pool}|fee={fee_str}")
        return " | ".join(parts)

    amount = Decimal(initial_amount)
    per_leg_cap: List[Decimal] = []
    for leg in legs:
        pool_state = pool_states.get(leg.pool)
        if not pool_state:
            return None
        zero_for_one = leg.token_in == pool_state.pool_meta.token0
        dec0 = pool_state.pool_meta.dec0
        dec1 = pool_state.pool_meta.dec1
        scale_in = Decimal(10) ** (dec0 if zero_for_one else dec1)
        scale_out = Decimal(10) ** (dec1 if zero_for_one else dec0)
        try:
            result = uni.simulate_swap(
                sqrt_price_x96=Decimal(pool_state.sqrt_price_x96),
                liquidity=Decimal(pool_state.liquidity),
                amount_in=amount * scale_in,
                zero_for_one=zero_for_one,
                fee_tier=pool_state.pool_meta.fee_tier,
                decimals_token0=dec0,
                decimals_token1=dec1,
            )
        except Exception:
            return None
        amount = result.amount_out / scale_out
        # price impact estimation
        try:
            price_before = uni.price_from_sqrt_price(Decimal(pool_state.sqrt_price_x96), dec0, dec1)
            price_after = uni.price_from_sqrt_price(result.sqrt_price_after, dec0, dec1)
            impact = abs(price_after - price_before) / price_before if price_before > EPS else Decimal(0)
            leg.price_impact = float(impact)
        except Exception:
            pass
        if amount <= EPS:
            return None
    roi = (amount - initial_amount) / initial_amount
    if roi > MAX_REALISTIC_ROI:
        log.warning("Unrealistic ROI %.6f high, legs=%s", roi, _legs_summary())
        return None
    if roi < MIN_REALISTIC_ROI:
        log.warning("Unrealistic ROI %.6f low, legs=%s", roi, _legs_summary())
        return None
    if roi < Decimal(str(min_roi)):
        return None

    # Size cap based on ROI break-even (across legs). Always use break-even search.
    size_cap = _cycle_cap_break_even(legs, pool_states, initial_amount)
    if size_cap is None or size_cap <= 0:
        size_cap = Decimal(initial_amount)

    return Opportunity(
        legs=legs,
        total_roi=float(roi),
        execution_cost=0.0,
        confidence=0.5,
        timestamp_ms=0,
        profit=float(amount - initial_amount),
        notional=float(initial_amount),
        size_cap=float(size_cap) if size_cap is not None else None,
    )


def _max_in_for_delta(pool_state: PoolState, delta: Decimal, zero_for_one: bool) -> Decimal | None:
    """Maximum token_in amount before price moves by given fractional delta.

    Uses single-range UniV3 math: target price = price*(1±delta). Returns amount in token_in units.
    """
    if delta <= 0:
        return None
    try:
        L = Decimal(pool_state.liquidity)
        sqrt_cur = Decimal(pool_state.sqrt_price_x96) / (Decimal(2) ** 96)
        dec0 = pool_state.pool_meta.dec0
        dec1 = pool_state.pool_meta.dec1
        scale = Decimal(10) ** (dec0 - dec1)
        price_cur = (sqrt_cur * sqrt_cur) * scale
        band = delta
        fee_fraction = Decimal(pool_state.pool_meta.fee_tier or 0) / Decimal(1_000_000)
        one_minus_fee = Decimal(1) - fee_fraction if fee_fraction < 1 else Decimal(1)
        if zero_for_one:
            price_target = price_cur * (Decimal(1) - band)
            if price_target <= 0:
                return None
            sqrt_target = (price_target / scale).sqrt()
            net_in = L * (sqrt_cur - sqrt_target) / (sqrt_cur * sqrt_target)  # token0 raw
        else:
            price_target = price_cur * (Decimal(1) + band)
            sqrt_target = (price_target / scale).sqrt()
            net_in = L * (sqrt_target - sqrt_cur)  # token1 raw
        # convert net (post-fee) to gross input by dividing by (1 - fee)
        gross_in = net_in / one_minus_fee if one_minus_fee > 0 else net_in
        scale_in = Decimal(10) ** (dec0 if zero_for_one else dec1)
        gross_human = gross_in / scale_in
        return gross_human if gross_human > 0 else None
    except Exception:
        return None


def _simulate_cycle_amount(start_amount: Decimal, legs: List[OpportunityLeg], pool_states: Dict[str, PoolState]) -> Decimal | None:
    amt = Decimal(start_amount)
    for leg in legs:
        pool_state = pool_states.get(leg.pool)
        if not pool_state:
            return None
        zero_for_one = leg.token_in == pool_state.pool_meta.token0
        # Abort if this size would cross the current tick boundary (single-range approximation)
        max_tick_amt = _max_in_to_tick_boundary(pool_state, zero_for_one)
        if max_tick_amt is not None and amt > max_tick_amt:
            return None
        try:
            dec0 = pool_state.pool_meta.dec0
            dec1 = pool_state.pool_meta.dec1
            scale_in = Decimal(10) ** (dec0 if zero_for_one else dec1)
            scale_out = Decimal(10) ** (dec1 if zero_for_one else dec0)
            amt_raw = amt * scale_in
            res = uni.simulate_swap(
                sqrt_price_x96=Decimal(pool_state.sqrt_price_x96),
                liquidity=Decimal(pool_state.liquidity),
                amount_in=amt_raw,
                zero_for_one=zero_for_one,
                fee_tier=pool_state.pool_meta.fee_tier,
                decimals_token0=dec0,
                decimals_token1=dec1,
            )
        except Exception:
            return None
        amt = res.amount_out / scale_out
        if amt <= EPS:
            return None
    return amt


def _cycle_cap_break_even(legs: List[OpportunityLeg], pool_states: Dict[str, PoolState], initial_amount: Decimal) -> Decimal | None:
    """
    Find the maximum starting amount where ROI >= 0 by binary searching on simulated swaps.
    No arbitrary band guard; relies solely on simulated slippage/fees.
    """

    MAX_LIMIT = Decimal("1e12")

    def roi_for(amount: Decimal) -> Decimal | None:
        out = _simulate_cycle_amount(amount, legs, pool_states)
        if not out:
            return None
        return (out - amount) / amount

    start = max(initial_amount, Decimal("1"))
    roi_start = roi_for(start)
    if roi_start is None:
        return None
    # If ROI is exactly zero (or nearly), cap is unbounded up to MAX_LIMIT
    if abs(roi_start) < Decimal("1e-12"):
        return start
    if roi_start < 0:
        return Decimal("0")

    low = Decimal("0")
    high = start

    # grow high until ROI <= 0 or we hit MAX_LIMIT
    for _ in range(60):
        roi_high = roi_for(high)
        if roi_high is None:
            return None
        if roi_high <= 0:
            break
        if high >= MAX_LIMIT:
            return MAX_LIMIT
        high *= 2
        if high > MAX_LIMIT:
            high = MAX_LIMIT
            roi_high = roi_for(high)
            if roi_high is None:
                return None
            if roi_high > 0:
                return MAX_LIMIT
            break

    roi_high = roi_for(high)
    if roi_high is None:
        return None

    # binary search for sign change
    for _ in range(80):
        mid = (low + high) / 2
        roi_mid = roi_for(mid)
        if roi_mid is None:
            return None
        if abs(roi_mid) < Decimal("1e-8"):
            return mid
        if roi_mid > 0:
            low = mid
        else:
            high = mid
        if high == 0:
            return Decimal("0")
        if (high - low) / high < Decimal("1e-6"):
            break
    return high


def _tick_spacing(fee_tier: int) -> int:
    return {100: 1, 500: 10, 3000: 60, 10000: 200}.get(fee_tier, 60)


def _max_in_to_tick_boundary(pool_state: PoolState, zero_for_one: bool) -> Decimal | None:
    """Maximum gross input before hitting the next tick boundary (single range).

    Uses current tick and fee tier. If tick is missing, returns None.
    """
    try:
        tick = pool_state.tick
        if tick is None:
            return None
        spacing = _tick_spacing(pool_state.pool_meta.fee_tier)
        if zero_for_one:
            target_tick = (tick // spacing) * spacing  # lower bound
        else:
            target_tick = (tick // spacing) * spacing + spacing  # upper bound
        sqrt_target = Decimal(uni.get_sqrt_ratio_at_tick(int(target_tick))) / (Decimal(2) ** 96)
        sqrt_cur = Decimal(pool_state.sqrt_price_x96) / (Decimal(2) ** 96)
        L = Decimal(pool_state.liquidity)
        fee_fraction = Decimal(pool_state.pool_meta.fee_tier or 0) / Decimal(1_000_000)
        one_minus_fee = Decimal(1) - fee_fraction if fee_fraction < 1 else Decimal(1)
        if zero_for_one:
            if sqrt_target >= sqrt_cur:
                return None
            net_in = L * (sqrt_cur - sqrt_target) / (sqrt_cur * sqrt_target)
        else:
            if sqrt_target <= sqrt_cur:
                return None
            net_in = L * (sqrt_target - sqrt_cur)
        gross = net_in / one_minus_fee if one_minus_fee > 0 else net_in
        scale_in = Decimal(10) ** (pool_state.pool_meta.dec0 if zero_for_one else pool_state.pool_meta.dec1)
        gross_human = gross / scale_in
        return gross_human if gross_human > 0 else None
    except Exception:
        return None
