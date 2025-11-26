import asyncio
import pytest

from v5.executor.null_executor import NullExecutor
from v5.common.models import Opportunity


@pytest.mark.asyncio
async def test_null_executor_returns_noop():
    exec = NullExecutor()
    opp = Opportunity(
        legs=[],
        total_roi=0.0,
        execution_cost=0.0,
        confidence=0.5,
        timestamp_ms=1,
    )
    result = await exec.execute(opp)
    assert result["status"] == "noop"
