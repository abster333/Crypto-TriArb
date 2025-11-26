"""Dry-run execution simulator."""

from __future__ import annotations

from decimal import Decimal
from v5.common.models import Opportunity


class ExecutionSimulator:
    async def simulate_execution(self, opp: Opportunity, gas_cost_usd: float = 1.0, slippage_bps: float = 5.0) -> dict:
        slippage = Decimal(1 - slippage_bps / 10_000)
        projected = Decimal(opp.total_roi + 1) * slippage - 1
        base_notional = Decimal(opp.notional or 0)
        profit_usd = float(base_notional * projected) if base_notional else 0.0
        profit_usd -= gas_cost_usd
        return {"profit_usd": profit_usd, "roi_effective": float(projected), "gas_cost_usd": gas_cost_usd}


__all__ = ["ExecutionSimulator"]
