"""Executor stub that uses the ExecutionSimulator for dry-run fills."""

from __future__ import annotations

import logging
from typing import Optional

from v5.common import metrics
from v5.common.models import Opportunity
from v5.executor.interface import Executor
from v5.simulator.execution_simulator import ExecutionSimulator

log = logging.getLogger(__name__)


class SimulatedExecutor(Executor):
    """A no-trade executor that estimates fill P&L."""

    def __init__(self, simulator: Optional[ExecutionSimulator] = None, gas_cost_usd: float = 1.0, slippage_bps: float = 5.0) -> None:
        self.simulator = simulator or ExecutionSimulator()
        self.gas_cost_usd = gas_cost_usd
        self.slippage_bps = slippage_bps

    async def execute(self, opportunity: Opportunity) -> dict:
        """Simulate execution and return projected economics."""
        result = await self.simulator.simulate_execution(opportunity, gas_cost_usd=self.gas_cost_usd, slippage_bps=self.slippage_bps)
        try:
            metrics.OPP_PROFIT_USD.labels(cycle="->".join([leg.token_in for leg in opportunity.legs])).set(result.get("profit_usd", 0))
        except Exception:
            pass
        log.info(
            "Simulated execution: roi_effective=%.5f profit_usd=%.2f gas=%.2f",
            result.get("roi_effective", 0.0),
            result.get("profit_usd", 0.0),
            self.gas_cost_usd,
        )
        return result


__all__ = ["SimulatedExecutor"]
