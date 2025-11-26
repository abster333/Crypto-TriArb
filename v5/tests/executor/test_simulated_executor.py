import pytest
from decimal import Decimal

from v5.executor.simulated_executor import SimulatedExecutor
from v5.simulator.execution_simulator import ExecutionSimulator
from v5.common.models import Opportunity, OpportunityLeg


@pytest.mark.asyncio
async def test_simulated_executor_returns_profit():
    sim = ExecutionSimulator()
    executor = SimulatedExecutor(simulator=sim, gas_cost_usd=1.0, slippage_bps=0.0)
    opp = Opportunity(
        legs=[
            OpportunityLeg(pool="0x" + "1"*40, token_in="A", token_out="B", amount_in=1.0, amount_out=1.01, price_impact=0.0),
            OpportunityLeg(pool="0x" + "2"*40, token_in="B", token_out="A", amount_in=1.01, amount_out=1.02, price_impact=0.0),
        ],
        total_roi=0.02,
        execution_cost=0.0,
        confidence=0.5,
        timestamp_ms=0,
        profit=0.02,
        notional=1.0,
    )
    res = await executor.execute(opp)
    assert "profit_usd" in res
    assert res["profit_usd"] != 0


@pytest.mark.asyncio
async def test_simulated_executor_handles_zero_notional():
    executor = SimulatedExecutor()
    opp = Opportunity(
        legs=[],
        total_roi=0.0,
        execution_cost=0.0,
        confidence=0.0,
        timestamp_ms=0,
        profit=0.0,
        notional=0.0,
    )
    res = await executor.execute(opp)
    assert res["profit_usd"] ==  - executor.gas_cost_usd
