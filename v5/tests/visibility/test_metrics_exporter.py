from v5.visibility.metrics_exporter import publish_pool_state, publish_opportunity
from v5.common.models import PoolMeta, PoolState, Opportunity, OpportunityLeg


def test_publish_pool_state_sets_metrics():
    meta = PoolMeta(pool_address="0x" + "1"*40, platform="UNI", token0="A", token1="B", fee_tier=500, network="eth")
    state = PoolState(pool_meta=meta, sqrt_price_x96="100", tick=1, liquidity="10", block_number=1, timestamp_ms=1)
    publish_pool_state(state)
    # no exception; optional checks on metric values


def test_publish_opportunity():
    leg = OpportunityLeg(pool="0x" + "1"*40, token_in="A", token_out="B", amount_in=1.0, amount_out=1.1, price_impact=0)
    opp = Opportunity(legs=[leg], total_roi=0.01, execution_cost=0.0, confidence=0.5, timestamp_ms=1, profit=0.0, notional=1.0, profit_usd=0.5)
    publish_opportunity(opp)
