import pytest
from pydantic import ValidationError

from v5.common.models import Token, PoolMeta, PoolState, DepthLevels, OpportunityLeg, Opportunity, HealthStatus, AlarmConfig


def test_token_validates_address_and_symbol():
    token = Token(address="0x" + "1"*40, symbol="eth", decimals=18, chain_id=1)
    assert token.address == "0x" + "1"*40
    assert token.symbol == "ETH"


def test_token_rejects_bad_address():
    with pytest.raises(ValidationError):
        Token(address="0x123", symbol="ETH", decimals=18, chain_id=1)


def test_poolmeta_uppercases_platform_network():
    meta = PoolMeta(pool_address="0x" + "2"*40, platform="uniswap_v3", token0="WETH", token1="USDC", fee_tier=500, network="eth")
    assert meta.platform == "UNISWAP_V3"
    assert meta.network == "ETH"


def test_depth_levels_positive_only():
    DepthLevels(bids=[(1.0, 1.0)], asks=[(2.0, 1.0)])
    with pytest.raises(ValidationError):
        DepthLevels(bids=[(-1.0, 1.0)], asks=[])


def test_pool_state_hex_validation():
    meta = PoolMeta(pool_address="0x" + "3"*40, platform="UNI", token0="A", token1="B", fee_tier=3000, network="eth")
    state = PoolState(pool_meta=meta, sqrt_price_x96="0x1", tick=0, liquidity="0x2", block_number=1, timestamp_ms=1)
    assert state.sqrt_price_x96 == "0x1"


def test_opportunity_validation():
    leg = OpportunityLeg(pool="0x" + "4"*40, token_in="A", token_out="B", amount_in=1.0, amount_out=1.1, price_impact=0.001)
    opp = Opportunity(legs=[leg], total_roi=0.01, execution_cost=0.0, confidence=0.9, timestamp_ms=10)
    assert opp.total_roi == 0.01
    with pytest.raises(ValidationError):
        Opportunity(legs=[leg], total_roi=-2, execution_cost=0.0, confidence=0.5, timestamp_ms=1)


def test_health_status_requires_address():
    with pytest.raises(ValidationError):
        HealthStatus(pool_address="bad", is_stale=False)


def test_alarm_config_ranges():
    cfg = AlarmConfig(webhook_urls=[], missing_data_threshold=1)
    assert cfg.missing_data_threshold == 1
    with pytest.raises(ValidationError):
        AlarmConfig(stale_snapshot_ms=-1)
