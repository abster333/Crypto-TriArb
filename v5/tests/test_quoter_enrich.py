import asyncio
from decimal import Decimal

import pytest
pytest.importorskip("pytest_asyncio")
import pytest_asyncio

from v5.common.models import PoolMeta, PoolState, OpportunityLeg, Opportunity, Token
from v5.simulator.realtime_detector import RealtimeArbDetector


class DummyScenario:
    def __init__(self, settings):
        self.settings = settings
        self.initial_amount = Decimal("10")
        self.tokens = {}


class DummySettings:
    def __init__(self):
        self.bsc_rpc_url = "http://dummy"


@pytest_asyncio.fixture(autouse=True)
def _install_anyio_backend():
    # ensure anyio backend available for asyncio tests
    import anyio
    return


@pytest.mark.asyncio
async def test_quoter_enrich_happy_path(monkeypatch):
    # Build detector with one pool and tokens
    meta = PoolMeta(
        pool_address="0x" + "1" * 40,
        platform="PANCAKE",
        token0="T0",
        token1="T1",
        fee_tier=100,
        network="BSC",
        dec0=18,
        dec1=18,
    )
    ps = PoolState(
        pool_meta=meta,
        sqrt_price_x96="79228162514264337593543950336",  # price=1
        tick=0,
        liquidity="1000000000000000000000000",
        block_number=1,
        timestamp_ms=0,
    )
    opp = Opportunity(
        legs=[
            OpportunityLeg(pool=meta.pool_address, token_in="T0", token_out="T1", amount_in=1.0, amount_out=1.0, price_impact=0.0)
        ],
        total_roi=0.01,
        execution_cost=0.0,
        confidence=1.0,
        timestamp_ms=0,
        profit=0.1,
        notional=10.0,
        size_cap=5.0,
    )

    detector = RealtimeArbDetector(
        scenario_runner=DummyScenario(DummySettings()),
        min_roi_threshold=0.0,
    )
    detector._pool_meta_by_addr = {meta.pool_address.lower(): meta}
    detector._tokens = {
        "T0": Token(address="0x" + "2" * 40, symbol="T0", decimals=18, chain_id=56),
        "T1": Token(address="0x" + "3" * 40, symbol="T1", decimals=18, chain_id=56),
    }

    # Mock quoter: return amount_out = amount_in * 1.01
    async def mock_quote(rpc_url, token_in, token_out, fee, amount_in, session=None, quoter=None, sqrt_price_limit_x96=0):
        return type(
            "Res",
            (),
            {
                "amount_out": int(Decimal(amount_in) * Decimal("1.01")),
                "sqrt_price_after": 0,
                "initialized_ticks_crossed": 0,
                "gas_estimate": 0,
            },
        )()

    monkeypatch.setattr("v5.simulator.quoter_client.quote_exact_input_single", mock_quote)

    res = await detector._quoter_enrich(opp)
    assert res
    assert res["amount_out"] > res["amount_in"]
    assert res["profit"] > 0
    assert res["per_leg"]
