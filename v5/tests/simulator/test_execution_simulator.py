import pytest

from v5.simulator.execution_simulator import ExecutionSimulator
from v5.common.models import Opportunity, OpportunityLeg


@pytest.mark.asyncio
async def test_execution_simulator():
    leg = OpportunityLeg(pool="0x" + "1"*40, token_in="A", token_out="B", amount_in=1.0, amount_out=1.1, price_impact=0)
    opp = Opportunity(legs=[leg], total_roi=0.01, execution_cost=0.0, confidence=0.5, timestamp_ms=1, profit=0.0, notional=100.0)
    sim = ExecutionSimulator()
    res = await sim.simulate_execution(opp)
    assert "profit_usd" in res
