import pytest
import asyncio
from decimal import Decimal

from v5.simulator.realtime_detector import RealtimeArbDetector
from v5.common.models import Opportunity, OpportunityLeg


class FakeRunner:
    def __init__(self, opps):
        self.opps = opps
    async def run_once(self):
        return self.opps


@pytest.mark.asyncio
async def test_detector_runs_and_stores_recent():
    leg = OpportunityLeg(pool="0x" + "1"*40, token_in="A", token_out="B", amount_in=1.0, amount_out=1.1, price_impact=0)
    opp = Opportunity(legs=[leg], total_roi=0.02, execution_cost=0.0, confidence=0.5, timestamp_ms=1, profit=0.0, notional=1.0)
    runner = FakeRunner([opp])
    det = RealtimeArbDetector(runner, min_roi_threshold=0.0, check_interval_ms=10)
    await det.start()
    await asyncio.sleep(0.05)
    await det.stop()
    assert det.recent_opportunities()


@pytest.mark.asyncio
async def test_detector_with_ws_trigger_like_update():
    # simulate detector seeing a new opportunity after "update"
    leg = OpportunityLeg(pool="0x" + "2"*40, token_in="A", token_out="B", amount_in=1.0, amount_out=1.1, price_impact=0)
    opp = Opportunity(legs=[leg], total_roi=0.5, execution_cost=0.0, confidence=0.5, timestamp_ms=1, profit=0.0, notional=1.0)
    runner = FakeRunner([opp])
    det = RealtimeArbDetector(runner, min_roi_threshold=0.0, check_interval_ms=5)
    await det.start()
    await asyncio.sleep(0.02)
    await det.stop()
    assert any(o.total_roi >= 0.5 for o in det.recent_opportunities())
